#!/usr/bin/env python3
"""claude_bridge — 스케줄러·시스템·음악·SNS 수집을 맡는 디스코드 봇(코어).

코어는 표준 라이브러리만 쓴다(외부 패키지 0). 플랫폼 종속은 `Adapter` 계층(adapter.py·
discord_adapter.py)이 흡수하고, 이 코어는 정규화 `Event`/`Button` 과 계약 메서드만 다룬다 —
플랫폼 교체 seam(현재 구현: 디스코드). 단일 워커가 이벤트를 직렬 처리한다: 인증 → 음악·청소·
재시작 명령 → 회신. 예약 알림·다이제스트(미국주식·스포티파이·SNS정보)는 별도 타이머 스레드.

보안 경계:
- user_id 허용목록 필수. 미허용 이벤트는 무회신·로그만.
- 메시지는 subprocess 리스트 인자(shell=False)로만 전달 — 셸 조립 금지.
- 봇 토큰은 .env·어댑터 내부에만. os.environ·로그·자식 프로세스 env 어디에도 넣지 않는다.
- claude 권한은 --allowedTools 최소 스코프 — **전 티어 Bash 0개**(임의 셸·git·네트워크 미부여).
  claude 는 미국주식 다이제스트의 뉴스 요약에만 쓰고, 프로젝트 원격 작업은 하지 않는다.
"""

from __future__ import annotations

import contextlib
import functools
import html
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ParamSpec, TypeGuard

import sns_inbox
import us_digest
import youtube
from adapter import _NOREDIRECT_OPENER, Adapter, Button, Event, _valid_id

# ── 경로 상수 ──────────────────────────────────────────────────────────────
PROJECT_DIR = Path(__file__).resolve().parent
LOG_DIR = PROJECT_DIR / "logs"
LOG_FILE = LOG_DIR / "bridge.log"
PID_FILE = LOG_DIR / "bridge.pid"
# 런처(start.ps1)가 "정말 접속했는가"를 볼 신호. Gateway on_ready 이후에만 생기고 종료 시 지운다.
# PID 파일로는 대신할 수 없다 — 그건 로그인 **전에** 잡는 락이라, 토큰이 거부돼도 잠깐 존재한다.
READY_FILE = LOG_DIR / "bridge.ready"
# 코드 변경 자동 재시작(run_loop.ps1 감독자와의 약속). 종료 전에 마커를 남기면 다음 기동이 🟢 알림을
# 생략하고 마커를 지운다. 종료 코드 RELOAD_EXIT_CODE(75) 는 감독자가 «즉시 재기동» 으로 읽는다.
RELOAD_MARKER = LOG_DIR / "reload_marker"
RELOAD_EXIT_CODE = 75
RELOAD_POLL_SEC = 10  # 코드 mtime 확인 간격
RELOAD_SETTLE_SEC = 5  # 마지막 변경 뒤 이만큼 조용해야 «저장 끝» 으로 본다(반쯤 쓰인 파일 방지)
SCHEDULES_FILE = PROJECT_DIR / "schedules" / "notify.json"
NOTIFY_STATE_FILE = LOG_DIR / "notify_state.json"
CHANNEL_MAP_FILE = LOG_DIR / "channel_map.json"  # channelID→(kind,tag) 매핑(자동생성 §4.4)
# ① 시각 알림용 상수. now·요일 판정은 항상 KST 기준(스케줄 at 은 KST HH:MM).
# KST 는 서머타임이 없어 고정 오프셋 +09:00 이면 충분 — ZoneInfo(IANA tz DB) 를 피해
# tzdata 미설치 Windows 노트북에서도 import 가 죽지 않게 한다(풀만으로 자동 실행).
_KST = timezone(timedelta(hours=9))
_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

NOTIFY_TICK_SEC = 25  # 알림 스케줄 주기 틱(§3.3 — poll 과 독립된 타이머 스레드)
LEAD_NOTIFY = "⏰"  # 예약 알림 본문 머리
# 음악 재생 명령 — 재생 자체는 플랫폼(디스코드 음성) 소관이라 코어는 명령 판정만 하고
# adapter.play_music/stop_music/skip_music capability 로 위임한다(clear_channel 패턴).
# 공백접기+casefold 단독 정확매칭 —
# 'ㅁ노래'·'ㅁ다음'·'ㅁ정지'만 발동(문장·평문은 미발동). 접두는 개인용 한글 자판 1키 'ㅁ' 통일.
MUSIC_PLAY_WORDS = frozenset({"ㅁ노래"})
MUSIC_SKIP_WORDS = frozenset({"ㅁ다음"})
MUSIC_STOP_WORDS = frozenset({"ㅁ정지"})
# 'ㅁ추가 <링크|검색어>' — 유튜브 재생목록 추가. 접두 매칭(뒤에 인자를 받음, 위 3종은 단독매칭).
MUSIC_ADD_WORDS = frozenset({"ㅁ추가"})
# 'ㅁ삭제 <제목>' — 유튜브 재생목록에서 제거(파괴적). 'ㅁ재생 <제목>' — 그 곡을 지금 재생.
# 둘 다 ㅁ추가와 같은 접두 매칭. ⚠️ 인가는 갈린다 — _playlist_bypass 는 ㅁ삭제만 우회에서 뺀다.
MUSIC_DEL_WORDS = frozenset({"ㅁ삭제"})
MUSIC_PLAY_ONE_WORDS = frozenset({"ㅁ재생"})
# 'ㅁ목록' — 재생목록 전곡 조회(읽기 전용·인자 없음). music_action 과 같은 공백접기 정확매칭.
MUSIC_LIST_WORDS = frozenset({"ㅁ목록"})
# 'ㅁ스포티파이' — kworb 미러의 스포티파이 주간차트 3개(글로벌·일본·한국) 상위 30곡씩을 한 번에
# 재생목록에 담는다. ㅁ목록과 같은 단독 정확매칭(인자 없음). ⚠️ 90곡을 밀어넣는 명령이라
# _playlist_bypass 우회에서 **뺀다**(ㅁ삭제·ㅁ목록과 같은 취급 — 허용목록 user 전용).
MUSIC_SPOTIFY_WORDS = frozenset({"ㅁ스포티파이"})


def music_action(text: str) -> str | None:
    """음악 명령 판정(순수). play|stop|skip|None. 공백접기+casefold 단독 정확매칭(문장 미발동)."""
    key = "".join(text.split()).casefold()
    if key in MUSIC_STOP_WORDS:
        return "stop"
    if key in MUSIC_SKIP_WORDS:
        return "skip"
    if key in MUSIC_PLAY_WORDS:
        return "play"
    return None


# 유튜브 URL 판정·videoId 추출(순수). 재생목록 전용 링크(v= 없음)는 extract 가 None → 개별 실패.
_YT_HOST_RE = re.compile(r"(?:youtube\.com|youtu\.be|music\.youtube\.com)", re.IGNORECASE)
_YT_ID_RE = re.compile(r"(?:v=|youtu\.be/|/shorts/|/embed/|/v/)([0-9A-Za-z_-]{11})")


def is_youtube_url(token: str) -> bool:
    """공백 구분 토큰이 유튜브 URL 인지(호스트 매칭)."""
    return bool(_YT_HOST_RE.search(token))


def extract_video_id(url: str) -> str | None:
    """유튜브 URL → 11자 videoId. watch?v=·youtu.be/·shorts·embed 지원. 재생목록만이면 None."""
    m = _YT_ID_RE.search(url)
    return m.group(1) if m else None


def _is_prefix_cmd(text: str, words: frozenset[str]) -> bool:
    """접두 명령 판정(순수) — 첫 토큰이 words 에 있으면 True. 붙여쓰기('ㅁ추가곡')는 미발동."""
    parts = text.strip().split(maxsplit=1)
    return bool(parts) and parts[0] in words


def _cmd_arg(text: str) -> str:
    """'ㅁ명령 <인자>' 의 인자 부분(공백 정리). 인자가 없으면 ''."""
    parts = text.strip().split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def is_music_add(text: str) -> bool:
    """'ㅁ추가' 명령 여부(접두 매칭 — 인자는 뒤에 붙는다). 'ㅁ추가곡' 등 붙여쓰기는 미발동."""
    return _is_prefix_cmd(text, MUSIC_ADD_WORDS)


def is_music_del(text: str) -> bool:
    """'ㅁ삭제' 명령 여부(접두 매칭). 재생목록에서 곡을 빼는 파괴적 명령 — 인가 우회 대상 아님."""
    return _is_prefix_cmd(text, MUSIC_DEL_WORDS)


def is_music_play_one(text: str) -> bool:
    """'ㅁ재생' 명령 여부(접두 매칭). 제목으로 그 곡을 지금 트는 명령(ㅁ노래=목록 전체 재생)."""
    return _is_prefix_cmd(text, MUSIC_PLAY_ONE_WORDS)


def is_music_list(text: str) -> bool:
    """'ㅁ목록' 여부(공백접기 정확매칭 — 인자 없는 명령). 재생목록 전곡을 번호·제목으로 회신."""
    return "".join(text.split()).casefold() in MUSIC_LIST_WORDS


def is_music_spotify(text: str) -> bool:
    """'ㅁ스포티파이' 여부(공백접기 정확매칭 — 인자 없는 명령). 'ㅁ스포티파이곡'은 미발동."""
    return "".join(text.split()).casefold() in MUSIC_SPOTIFY_WORDS


# ── 'ㅁ추가 <검색어>' 검색 결과 선택(순수 — 네트워크는 어댑터 몫) ─────────────────
# 검색 1위를 그대로 넣으면 방송무대·팬편집이 섞인다. 근거는 **검색 결과**다 — 2026-08-18 에
# 재생목록의 방송무대 11곡(열린음악회·스케치북·Mnet 라이브와이어·교차편집·Choreography·@TOUR·
# 비긴어게인·[최초 공개]·[DF LIVE])을 관측해 규칙을 세웠고, 같은 날 그 11곡을 원곡 버전으로
# **교체**했다. ⚠️ 그래서 «재생목록 99곡 중 11곡» 으로 재확인할 수 없다 — 지금 재생목록에는
# 그 11곡이 없다(2026-08-18 실측 = **100곡 중 적중 0**). 이 상수를 손댈 때의 회귀 기준은
# ① 재생목록 전곡에 적중 0(오검출 없음) ② 검색어 25건의 선택 결과가 검색어와 겹칠 것 두 가지다.
# 🔴 'cover'·'커버'·'가사'·'lyric' 을 넣지 마라 — 가사영상 54곡·커버 2곡은 개발자가 **의도해서**
# 넣은 것이라, 거르는 순간 목록의 절반이 밀려난다.
_STAGE_CHANNEL_RE = re.compile(
    # 🔴 방송사는 «채널명 전체»가 아니라 낱말로 잡는다 — 2026-08-18 실측에서
    #    `SBS Entertainment` 가 `SBS ?K` 를 비껴가 방송 무대가 1위로 뽑혔다.
    #    `KBS부산`·`Mnet TV` 처럼 접미가 자유로워 접두만 고정한다.
    r"KBS|SBS|MBC|Mnet|Beginagain|비긴어게인|방구석|TV ?CHOSUN|JTBC",
    re.IGNORECASE,
)
_STAGE_TITLE_RE = re.compile(
    r"교차편집|stage ?mix|@.{0,20}tour|최초 ?공개|열린음악회|스케치북|"
    r"라이브와이어|choreography|직캠|fancam|"
    # 🔴 'live' 는 **단독으로 잡지 않는다** — 곡명에 그대로 쓰인다(실측: `Oasis - Live Forever`
    #    가 5후보 전부 적중해 필터가 무의미해졌다). 라이브 세션·클립 문맥일 때만 잡는다.
    #    ⚠️ 'special clip' 은 **제거**했다 — 레이블 공식 음원 표기다(실측: `[Official] DK(디셈버)
    #    - 행복하지 말아요 (Special Clip)` 이 걸러져 무관한 「260717 Havy V7」이 뽑혔다).
    #    채널 조건과 AND 로 묶는 안은 무의미하다 — 방송사 채널이면 _STAGE_CHANNEL_RE 가 이미 잡는다.
    r"\(live\)|\[live\]|live ?(?:clip|session|stage|performance|ver)|df ?live|"
    r"라이브 ?(?:클립|세션|무대|영상)|"
    # 루프 영상 — 원곡이 아니라 「1시간 연속 재생」류다(2026-08-18 실측: 대체 후보 1위로 뽑혔다).
    # 🔴 시간·hour 는 **루프 문맥을 요구**한다. 맨 `[0-9]+ ?시간` 은 곡명을 때린다
    # (실측: `선미 - 24시간이 모자라`·`가인 - 24 시간이 모자라` 가 5후보 전부 적중).
    r"[0-9]+ ?시간 ?(?:연속|반복|재생|듣기|/ ?[0-9]+ ?hour)|[0-9]+ ?hours? ?(?:loop|non-? ?stop)|"
    r"연속 ?재생|연속 ?듣기|반복 ?듣기|반복 ?재생|"
    # 음악방송 프로그램명 — 팬이 올려 채널이 방송사가 아닌 경우가 많아 제목으로 잡는다
    # (2026-08-18 실측: 「2018 가온 차트 무대 / Gaon Chart Stage」가 대체 후보로 뽑혔다).
    r"가온 ?차트|gaon ?chart|차트 ?무대|뮤직뱅크|music ?bank|음악중심|인기가요|"
    # `콘서트` 는 2026-08-18 재검토에서 **남겼다** — 실측 25건 중 적중은 실제 공연 실황
    # (`임재범 - 너를 위해 / 2016 Tour In Seoul 30주년 기념 콘서트`) 하나뿐이었고 오검출은 0.
    # 사용자가 «콘서트» 를 직접 치는 경우는 pick_index 의 query 우회가 받는다.
    r"엠카운트다운|m ?countdown|쇼챔피언|더 ?쇼|가요대전|가요대축제|콘서트",
    re.IGNORECASE,
)
# 'ㅁ추가 <검색어> #2' — 마지막 토큰이 #숫자면 순번 지정. '#' 없는 숫자는 검색어의 일부다
# ('ㅁ추가 소녀시대 999' 는 곡 제목). ASCII 숫자만 받는다(전각 숫자로 순번을 지정할 길 없음).
# 🔴 **1~99 만 순번**이다 — `#0` 은 순번이 아닌데 [0-9]{1,2} 로 받으면 index==0(=미지정)이 되어
# 「필터가 알아서 고름」으로 조용히 바뀌었다. `#100` 이 검색어에 남는 것과 같은 취급으로 통일한다.
_ADD_INDEX_RE = re.compile(r"^(?P<query>.+?)\s+#(?P<n>[1-9][0-9]?)$")


def parse_add_index(arg: str) -> tuple[str, int]:
    """'ㅁ추가' 인자 → (검색어, 순번). 순번 미지정은 0(=필터가 고른다). 순수 함수."""
    m = _ADD_INDEX_RE.match(arg.strip())
    if m is None:
        return (arg.strip(), 0)
    return (m.group("query").strip(), int(m.group("n")))


def is_stage_clip(title: str, channel: str = "") -> bool:
    """검색 결과가 방송무대·팬편집·라이브세션인지(순수). 위 두 정규식 근거 주석 참조."""
    return bool(_STAGE_CHANNEL_RE.search(channel) or _STAGE_TITLE_RE.search(title))


def pick_index(
    candidates: list[tuple[str, str, str]], index: int = 0, query: str = ""
) -> int | None:
    """후보 (videoId, 제목, 채널) 목록에서 고를 위치(0-based). 후보 없으면 None.

    `index >= 1`: 사용자가 '#N' 으로 지정한 순번(1-based, parse_add_index 가 1~99만 넘긴다) —
    **필터를 무시**하고 그대로 쓴다. 범위 밖이면 None(잘못된 번호를 조용히 바꿔치지 않는다).
    `index == 0`(미지정): 필터에 안 걸리는 첫 후보. **전부 걸리면 1위로 폴백**한다 — 요청은
    '넣어달라'였으므로 추가 자체를 막지 않는다(원치 않으면 '#N' 으로 다시 고른다).
    🔴 `query` 자체가 필터 규칙에 걸리면 **필터를 끈다**(1위 그대로). 그 낱말은 사용자가 직접
    친 것이라 「형식 잡음」과 구분할 근거가 없다 — 실측: `24시간이 모자라`·`Live Forever` 는
    5후보가 **전부** 적중해 필터가 무의미해지고, 우연히 순서가 바뀌면 무관한 영상이 뽑혔다.
    """
    if not candidates:
        return None
    if index > 0:
        return index - 1 if index <= len(candidates) else None
    if query and is_stage_clip(query):
        return 0
    for i, (_vid, title, channel) in enumerate(candidates):
        if not is_stage_clip(title, channel):
            return i
    return 0


# ── 곡 제목 표시 정리(순수 — **표시 전용**) ────────────────────────────────────
# 🔴 재생목록의 실제 제목·'ㅁ삭제' 제목 매칭(youtube.remove_video → fold_title)·'ㅁ목록' 회신은
#    **원본 제목 그대로** 써야 한다. 거기에 이 정리본을 끼우면 제목이 서로 달라져 매칭이 깨진다.
#    쓰는 곳은 곡 전환 알림(discord_adapter._play_current) 하나다.
# 아래 패턴과 적용 순서는 2026-08-18 실제 재생목록 99곡에 전부 대보고 확정했다(감으로 고치지 말 것).
_HANGUL = re.compile(r"[가-힣]")
# [Music Video]·【MV】 — 대괄호류는 실측 99곡에서 예외 없이 부가표기였다(곡명에 쓰인 적 없음).
# 전각 대괄호(U+FF3B/FF3D)도 함께 받는다 — 한글 자판 업로더가 섞어 쓴다.
_T_BRACKET = re.compile(r"[\[\【\uff3b][^\]\】\uff3d]*[\]\】\uff3d]")
# 괄호 부가표기: (Official MV)·(feat. …)·(OVAN) 같은 영문 병기. 마지막 대안지는 「ASCII·기호뿐인
# 괄호」라 한글이 든 괄호(곡명의 일부일 수 있다)는 남긴다.
_T_NOISE_PAREN = re.compile(
    r"\((?:[^()]*?)(?:official|mv|video|audio|lyric|가사|feat\.?|prod|inst|"
    r"[A-Za-z0-9 .,'&!?-]+)\)",
    re.IGNORECASE,
)
# 홍보 문구 구분자 — `ㅣ`·`|`·전각 U+FF5C + **공백으로 둘러싸인 슬래시**.
# 🔴 붙여 쓴 슬래시는 넣지 마라 — 곡명 `O/W` 가 `O` 로 잘린다(2026-08-18 실측 사고).
_T_SEG_SPLIT = re.compile(r"[ㅣ|\uff5c]|\s+/\s+")
_T_DECOR = re.compile(r"[♬♪🔗❣️⭐️]+")  # 업로더 장식 문자 — 정보가 없다
# 「가수 - 곡명」 구분자 — 하이픈·en대시(U+2013)·em대시(U+2014) 를 **같게** 본다. 업로더가 섞어
# 써서 하이픈만 보면 `백지영 (U+2013) 다시는 사랑하지 않고` 가 「가수 없음」으로 떨어졌다(실측).
# 🔴 대시를 보는 규칙은 **이 문자클래스 하나를 공유한다** — 따로 적으면 하나가 빠진다(실측: 종전
# `_TOPIC_SUFFIX` 는 리터럴 `" - Topic"` 이라 `ZUTOMAYO (U+2013) Topic` 을 Topic 채널로 못 봤다).
# (이스케이프로 쓴다 — 세 글자는 소스에서 눈으로 구분되지 않는다)
_DASH = r"[-\u2013\u2014]"
_T_DASHED = re.compile(rf"\s{_DASH}\s")  # 판정: 「A - B」 꼴인가
_T_PART_SPLIT = re.compile(rf"\s+{_DASH}\s+|\s+_\s+")  # 분할: 아티스트/곡명
_T_STRIP = " -_|·\u2013\u2014"  # 조각 양끝에서 떼는 구분자 잔재
# 조각 끝에 남은 `가사`·`Lyrics` 류 꼬리표(가사영상이 목록의 절반이라 흔하다).
# 🔴 `+` 로 **반복** 매칭한다 — 1회만 지우면 `… 가사 해석` 에서 `가사` 가 남는다(실측).
_T_TAIL_TAG = re.compile(r"(?:[ ,/]*(?:가사|해석|발음|lyrics?|inst))+\s*$", re.IGNORECASE)


def _drop_ascii_tail(seg: str) -> str:
    """한글 조각 꼬리에 붙은 영문 병기를 뗀다(`행복 Happiness` → `행복`). 순수.

    🔴 **곡명 조각(clean_track_title 의 i>=1)에만 쓴다.** 아티스트 조각에 쓰면 이름이 통째로
    날아간다 — 실측 사고: `어서 날아가렴, Florina - Va Va Vis` 에서 `Florina` 가 소실됐다.
    한글이 없는 조각(원래 영어 곡)은 손대지 않는다 — 다 떼면 곡명이 빈다.
    """
    if not _HANGUL.search(seg):
        return seg  # 영어 곡 원본 보존
    words = seg.split()
    # 꼬리부터 벗긴다. len>1 조건으로 최소 한 낱말은 남겨 조각이 통째로 비는 것을 막는다.
    while (
        len(words) > 1
        and not _HANGUL.search(words[-1])
        and re.fullmatch(r"[A-Za-z0-9'&.,!?-]+", words[-1])
    ):
        words.pop()
    return " ".join(words)


def clean_track_title(title: str) -> str:
    """유튜브 제목 → 「아티스트 - 곡명」 표시용 정리(순수). 정리 결과가 비면 **원본을 그대로** 반환.

    예: `오반 (OVAN) - 행복 Happiness [Music Video]` → `오반 - 행복`.
    폴백(원본 반환)이 있어야 알림이 빈 줄로 나가지 않는다.
    """
    title = title[:300]  # 길이 상한 — 아래 정규식이 겹쳐 최악 O(n²) 다(4000자 = 370ms 실측).
    # 코어는 단일 스레드라 그동안 다른 이벤트가 멈춘다. 표시 한도가 100자(escape_reply)라 손실 0.
    # 「유튜브 제목은 100자」라는 정책에만 기대던 방어선을 코드로 들인다.
    t = _T_DECOR.sub(" ", title)
    t = _T_BRACKET.sub(" ", t)
    for _ in range(3):  # (A)(B)(C) 처럼 여러 번 붙는다 — 중첩이 아니라 반복이라 3회로 충분(실측)
        t = _T_NOISE_PAREN.sub(" ", t)
    # 곡선 따옴표(U+2019/U+2018) → 직선(아래서 함께 제거). 이스케이프로 쓴다 — 소스에 그대로
    # 넣으면 직선 따옴표와 눈으로 구분되지 않아 나중에 잘못 고쳐진다(ruff RUF001 도 같은 이유).
    t = t.replace("\u2019", "'").replace("\u2018", "'")
    t = re.sub(r"['\"]", "", t)  # 따옴표 표기가 업로더마다 제각각이라 지워서 통일한다
    # 'ㅣ'·'|'·' / ' 로 이어붙인 홍보 문구. 🔴 **「A - B」 꼴 조각을 고른다** — 앞 조각만 취하면
    # 곡명이 통째로 사라진다(실측 사고: `역주행 가능성 58000퍼센트 | 케이시 (Kassy) - 사진첩`,
    # `노래모음 / 케이시 (Kassy) - 사진첩`). 슬래시도 같은 분기로 처리한다 — 종전의 «꼬리부터
    # 잘라내기»는 `A / B` 에서 **앞 조각**을 남겨 곡명 쪽을 버렸다.
    segs = [x.strip() for x in _T_SEG_SPLIT.split(t) if x.strip()]
    # 🔴 **ASCII 하이픈 조각을 먼저** 고르고, 하나도 없을 때만 en/em대시로 넓힌다 — **분할 확장과
    # 조각 선택 확장은 다르다.** `max(len)` 은 «대시 든 조각은 하나뿐» 을 전제하는데, 세 대시를
    # 같게 보면 홍보 조각이 후보에 들어와 **더 길어서 이긴다**(실측 회귀:
    # `아이유 - 좋은날 ㅣ … 플레이리스트 (U+2013) 노래 모음 best` 의 곡명이 홍보 문구로 바뀌었다).
    dashed = [x for x in segs if " - " in x] or [x for x in segs if _T_DASHED.search(x)]
    t = max(dashed, key=len) if dashed else (segs[0] if segs else t)
    # 아티스트/곡명 분해. `_` 구분자도 받는다(업로더가 `아티스트 _ 곡명` 으로 쓰는 경우가 있다).
    parts = [p.strip() for p in _T_PART_SPLIT.split(t) if p.strip()]
    parts = [_drop_ascii_tail(p) if i else p for i, p in enumerate(parts)]  # i==0(아티스트)은 보존
    parts = [re.sub(r"\s{2,}", " ", p).strip(_T_STRIP) for p in parts if p.strip(_T_STRIP)]
    parts = [_T_TAIL_TAG.sub("", p).strip() for p in parts]
    parts = [p for p in parts if p]  # 위 제거로 빈 조각이 생길 수 있다
    # 3조각 이상이면 앞 2개(아티스트·곡명)만 — 뒤는 부제·설명이다.
    out = " - ".join(parts[:2]) if len(parts) >= 2 else (parts[0] if parts else title)
    return re.sub(r"\s{2,}", " ", out).strip() or title


# 유튜브 자동생성 아티스트 채널(`더 크로스 - Topic`) 판정. 대시 3종을 위와 **같은 클래스**로 본다.
_TOPIC_CHANNEL = re.compile(rf"\s{_DASH}\sTopic$")
_ARTIST_LIMIT = 30  # 회신에 싣는 가수 상한 — 긴 채널명이 한 줄을 먹어 곡명이 밀리면 본말전도다


def display_title(title: str, channel: str = "") -> str:
    """회신·알림에 싣는 표시 제목(순수) — `clean_track_title` + **Topic 채널일 때만** 가수 채우기.

    ⚠️ **표시 전용 휴리스틱이다.** `<가수> - Topic` 은 유튜브가 자동생성하는 아티스트 채널의 관례적
    형태지만, **유튜브가 이 접미사를 예약한다는 근거는 확인하지 못했다** — 누구나 채널명을 그렇게
    지을 수 있고 그것을 막는 코드도 없다. 실측 101곡이 증명한 것은 «Topic 이 아닌 채널에 붙이면
    틀린다» 이지 그 역이 아니다. 틀려도 손해는 **표시 제목 한 줄**이다(매칭·삭제는 원본 제목으로
    한다 — 위 정리 블록 주석).
    정리 결과에 「가수 - 곡명」 구분자가 없을 때만 그 가수를 앞에 붙인다(`사랑하니까` →
    `더 크로스 - 사랑하니까`).
    🔴 **Topic 이 아닌 채널로 넓히지 마라 — 틀린 가수를 붙인다.** 실측(재생목록 101곡): 가수가
    안 붙는 21곡 중 Topic 은 2곡뿐이고 나머지 19곡은 가사채널·팬업로드라 채널명이 가수가 아니다
    (`Magical Syndrome`/채널 `글집`, `DAY6 Sweet Chaos`/채널 `Lemoring`).
    채널을 모르는 경로(`ㅁ추가 <URL>`·채널 없는 엔트리)는 정리된 제목 그대로 — 폴백이 기본값이다.
    제목이 비거나 **공백뿐이면 '' 를 준다** — 호출부의 `or videoId` 폴백이 살아나야 한다.
    """
    t = clean_track_title(title).strip()
    topic = _TOPIC_CHANNEL.search(channel)
    if not t or topic is None or _T_DASHED.search(t):
        return t
    # 🔴 채널명에 clean_track_title 을 태우지 마라 — **제목용** 규칙이라 홍보 문구 분할이 걸려
    # `츄ㅣ츄 - Topic` 이 `츄` 로 잘린다. 접미사를 떼고 공백만 정리한다.
    artist = re.sub(r"\s{2,}", " ", channel[: topic.start()]).strip()
    # 안 붙이는 경우: ① 가수가 없다 ② 채널명 안에 또 구분자가 있다(`A - B - Topic` — 「가수 -
    # 곡명」 2조각 불변식이 깨진다) ③ 곡명이 이미 그 가수로 시작한다(`아이유 - 아이유 좋은날`).
    if not artist or _T_DASHED.search(artist) or t.casefold().startswith(artist.casefold()):
        return t
    return f"{artist[:_ARTIST_LIMIT].strip()} - {t}"


# 회신에 실을 때 지워야 하는 제어·서식 문자: 개행(가짜 UI 를 만든다)·제로폭·양방향 제어 전부.
# 🔴 **범위를 좁히지 마라.** 종전엔 `\r\n`·U+200B~200F·U+202A~202E 뿐이라 아래 docstring 이 선언한
# 「양방향 override 차단」이 절반만 됐다 — Trojan Source 가 실제로 쓰는 isolate 4종(U+2066~2069)·
# 아랍문자 마크(U+061C)·줄/문단 구분자(U+2028·2029)·soft hyphen(U+00AD)·BOM(U+FEFF)·태그 문자
# (U+E0000~E007F)가 그대로 통과했다(2026-08-18 실측).
# (이스케이프로 쓴다 — 소스에 실문자로 넣으면 **눈에 보이지 않아** 나중에 지워진다)
_UNSAFE_CTRL = re.compile(
    r"[\r\n\u00ad\u061c\u200b-\u200f\u2028\u2029\u202a-\u202e"
    r"\u2060-\u2064\u2066-\u206f\ufeff\U000e0000-\U000e007f]"
)


def escape_reply(text: str, limit: int = 100) -> str:
    """★ 외부 문자열을 봇 회신에 실을 때의 **유일한** 안전 표기(보안 감사 대상). 빈 값은 ''.

    감싸는 대상은 두 가지이고 **위협은 같다**:
      · 사용자 입력(`ㅁ추가 <검색어>`) — 이 채널은 비인가 서버 멤버도 쓴다(_playlist_bypass)
      · 🔴 **제3자가 올린 유튜브 제목**(과 그 **채널명** — 가수 채우기로 회신에 실리는 새 입력원)
        — `ㅁ목록`·추가/삭제 결과·곡 전환 알림으로
        나간다. 멤버 누구나 `ㅁ추가` 로 「제목이 마크다운 링크인 영상」을 넣으면 **봇 명의로**
        피싱 링크가 게시되고, 곡이 돌 때마다 다시 뜬다. `ㅁ삭제` 는 인가가 필요해 공격자는
        지우지도 못한다(멘션은 클라이언트에서 전면 차단했지만 링크·서식은 아니다).
    규칙: 백틱 제거(코드스팬 탈출) → 제어문자 제거 → 길이 절단 → 코드스팬으로 감싸기.
    ⚠️ **호출부에서 백틱을 덧붙이지 마라** — 이 함수가 감싼다(종전엔 docstring 만 그렇게 적혀
    있고 실제로는 호출부 2곳이 붙여, 새 호출부가 생길 때마다 잊혔다).
    """
    t = _UNSAFE_CTRL.sub("", text.replace("`", ""))[:limit].strip()
    return f"`{t}`" if t else ""


def pack_lines(lines: list[str], limit: int) -> list[str]:
    """줄 목록 → 한도 이하 메시지들(순수). 줄 중간을 자르지 않고 줄 경계로만 나눈다.

    한 줄이 한도를 넘으면 그 줄만 단독 메시지로 둔다(어댑터의 chunk_text 가 마지막에 자른다).
    """
    out: list[str] = []
    buf: list[str] = []
    size = 0
    for line in lines:
        if buf and size + len(line) + 1 > limit:
            out.append("\n".join(buf))
            buf, size = [], 0
        buf.append(line)
        size += len(line) + 1
    if buf:
        out.append("\n".join(buf))
    return out


# 명령 접두 'ㅁ' 통일(개인용 — 한글 자판 1키). 슬래시('/help')·접두 없는 평문('청소')은 명령이
# 아니다. 동의어만 별칭으로 두고 정규 ㅁ 토큰으로 접는다.
COMMAND_ALIASES = {"ㅁ사용법": "ㅁ도움말"}
# 정규 ㅁ 명령 토큰(별칭 접힘 후 라우팅이 == 로 비교하는 값) + 동의어. help 폴백
# (알 수 없는 ㅁ… → HELP)이 정규 명령을 오검출하지 않게 한다.
COMMANDS = frozenset({"ㅁ도움말", "ㅁ재시작", "ㅁ청소"}) | frozenset(COMMAND_ALIASES)
# 플레이리스트 전용 채널(🎵 PlayList) — 사람끼리 대화하는 공간이라 화이트리스트(음악 재생·청소·
# ㅁ추가)만 처리하고 그 외는 반응·안내 없이 조용히 무시한다(_handle_text 최상단 게이트). 태그는
# _ensure_voice 가 durable 하게 관리하는 "playlist"(계약의 '플레이리스트'는 이 내부 태그로 실현).
_MUSIC_ONLY_ROLES = frozenset({"playlist"})
# 'ㅁ목록' 회신 1건의 최대 길이. 디스코드 한도 2000 보다 낮춰 잡는다 — 어댑터가 마스킹(***)으로
# 길이를 늘릴 수 있고, 한도에 딱 붙이면 그 증가분이 chunk_text 의 무자비한 절단으로 되돌아온다.
MUSIC_LIST_MSG_LIMIT = 1800


log = logging.getLogger("bridge")

# ① 알림 상태 — logs/notify_state.json 에 영속. 타이머 스레드(dispatch)와 다이제스트 워커가 공유
# 하므로 _notify_lock 으로 보호한다(§3.3 타이머 스레드 도입으로 필요).
# ponytail: 프로세스 1개·저빈도라 굵은 단일 락으로 충분 — 경합 병목 시 세분화.
notify_fired: set[tuple[str, str]] = set()  # (id, "YYYY-MM-DD") — 오늘 발송 완료분
_notify_lock = threading.Lock()

# 다이제스트 실패 되돌림 횟수 {(id, "YYYY-MM-DD"): n} — **id 별로** 센다. 키가 날짜뿐이면
# 같은 틱에 함께 도는 다른 다이제스트가 예산을 나눠 써, 한쪽이 3번 실패하면 다른 쪽은 한 번도
# 시도되지 못하고 그날 포기된다. 오늘 것만 남긴다(_revert_digest_fired 가 갱신 시 정리).
_digest_attempts: dict[tuple[str, str], int] = {}


# ══════════════════════════════════════════════════════════════════════════
# 순수 함수 (qa 병렬 테스트 대상 — 시그니처 고정)
# ══════════════════════════════════════════════════════════════════════════
def is_allowed(chat_id: int, allowed: frozenset[int]) -> bool:
    """chat_id 가 허용목록에 있는지."""
    return chat_id in allowed


_warned_session_ids: set[str] = set()  # on:"session" 경고를 id 당 1회로 줄이는 기록


def load_schedules(path: Path) -> list[dict[str, Any]]:
    """notify.json → items 리스트. 파일 없음·손상은 빈 리스트(load_env 로더처럼 방어적).

    timezone 필드는 향후 확장용 예약 — 현재는 _KST(Asia/Seoul) 고정이라 읽지 않는다(YAGNI).
    id 가 안전 규칙(_valid_id) 위반인 항목은 조용히 skip(로더 방어 스타일 — callback 계약 보호).
    `on:"session"` 항목(세션 핑 경로는 2026-10-09 삭제)은 **버리되 id 당 한 번 경고**한다 —
    이 로더는 매 틱 불려서 매번 찍으면 도배가 되고, 조용히 버리면 «왜 안 오지» 가 된다.
    """
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):  # ValueError = JSONDecodeError · UnicodeDecodeError(비-UTF8)
        return []
    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        return []
    valid = [it for it in items if isinstance(it, dict) and _valid_id(it.get("id"))]
    kept = []
    for it in valid:
        if it.get("on") == "session":
            if it["id"] not in _warned_session_ids:
                _warned_session_ids.add(it["id"])
                log.warning("notify.json %s: on='session' 지원 종료 — 무시한다", it["id"])
            continue
        kept.append(it)
    return kept


def due_notifications(
    items: list[dict[str, Any]],
    now_kst: datetime,
    fired: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    """지금(now_kst) 발송할 스케줄 항목 반환. 순수(부작용 없음, now·fired 를 인자로 받음).

    조건: now 의 요일이 항목 days 에 있고, now 가 [at, at+grace_min] 창 안이며
    (id, 오늘날짜) 가 fired 에 없음. now_kst 는 tz-aware KST 를 받는다. at·grace_min 이
    깨진 항목은 조용히 skip(브리지 안 죽게 — 로더와 같은 방어적 태도).
    """
    day = _WEEKDAYS[now_kst.weekday()]
    today = now_kst.date().isoformat()
    out: list[dict[str, Any]] = []
    for it in items:
        item_id = it.get("id")
        if not isinstance(item_id, str):
            continue
        days = it.get("days")
        at = it.get("at")
        if not isinstance(days, list) or day not in days:
            continue
        if not isinstance(at, str) or ":" not in at:
            continue
        parts = at.split(":")
        try:
            hh, mm = int(parts[0]), int(parts[1])
        except (ValueError, IndexError):
            continue
        grace = it.get("grace_min", 30)
        if not isinstance(grace, int):
            grace = 30
        try:
            start = now_kst.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except ValueError:
            continue  # 24:00 등 범위 밖 시각
        if start <= now_kst <= start + timedelta(minutes=grace) and (item_id, today) not in fired:
            out.append(it)
    return out


def notice_stamp(now: datetime | None = None) -> str:
    """시스템 소식·예약 알림 머리 `[YYYY-MM-DD AM hh:mm]`(KST·12시간제).

    문구 = 개발자 확정(편집기). 모든 시스템 소식이 이 머리로 시작한다.
    """
    t = now or datetime.now(_KST)
    return f"[{t:%Y-%m-%d} {'AM' if t.hour < 12 else 'PM'} {t.hour % 12 or 12:02d}:{t:%M}]"


def off_notice_text(at: datetime | None = None) -> str:
    """🔴 꺼짐 알림(개발자 확정).

    `ㅁ재시작` 직전, 또는 10분+ 끊겼다 돌아왔을 때 «끊긴 시각»으로 보낸다.
    """
    return f"{notice_stamp(at)} 🔴 bridge Off"


def notify_text(it: dict[str, Any], now: datetime | None = None) -> str:
    """예약 알림 본문(버튼 없음). `[시각] ⏰ <스케쥴> {label}` + note 가 있으면 `\\n→ {note}`."""
    label = str(it.get("label", "")).strip()
    note = str(it.get("note", "")).strip()
    head = f"{notice_stamp(now)} {LEAD_NOTIFY} <스케쥴> {label}".rstrip()
    return f"{head}\n→ {note}" if note else head


# ══════════════════════════════════════════════════════════════════════════
# 설정 · 저장소 상태
# ══════════════════════════════════════════════════════════════════════════
_SECRET_MIN_LEN = 12  # 마스킹 대상 .env 값의 최소 길이(짧은 값이 정상 텍스트를 갈아엎지 않게)
# 길이가 길지만 **비밀이 아닌** 설정 키 — 값이 회신 본문에 정상적으로 등장한다(경로·URL·초).
# 예: TARGET_ROOT="Hachiware/_Project" 를 마스킹하면 `M ***/etf-info/app.py` 처럼 모든 원격
# 작업 회신의 파일 경로가 깨진다. **제외 목록(블랙리스트가 아닌 예외)** 방식인 이유: 키 화이트
# 리스트(*TOKEN|SECRET|KEY 만 마스킹)로 뒤집으면 새 비밀 키가 추가될 때 **조용히 마스킹에서
# 빠진다**. 여기 안 적힌 값은 전부 마스킹되므로 누락 시 최악이 "과잉 마스킹"에 그친다(fail-safe).
_SECRET_SKIP_KEYS = frozenset({"TARGET_ROOT", "CLAUDE_TIMEOUT_SEC", "MUSIC_PLAYLIST_ID"})


def load_env(path: Path) -> dict[str, str]:
    """.env 직접 파싱(KEY=VALUE, # 주석·빈 줄 무시, 양끝 따옴표 제거)."""
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        env[key.strip()] = val.strip().strip('"').strip("'")
    return env


def build_secrets(token: str, repo_root: Path, env: dict[str, str]) -> list[str]:
    """회신 마스킹 대상(adapter.secrets) — 봇 토큰 · 내부 절대경로 · `.env` 값 전부. 순수.

    헤드리스 claude 는 cwd 가 워크스페이스 안이면 Read 사정거리에 브리지 `.env`·
    `.oauth_token.json` 이 있다. 외부 텍스트 인젝션이 성공하면 회신 본문으로
    비밀값이 새어 나올 수 있으므로 토큰 하나가 아니라 **.env 값 전부**를 마스킹 대상에 넣는다.
    12자 미만은 제외 — 포트·플래그 같은 짧은 값이 정상 텍스트를 `***` 로 갈아 회신을 파괴한다.
    길지만 비밀이 아닌 설정 키(`_SECRET_SKIP_KEYS`)도 제외 — 같은 이유(회신 경로 훼손).
    빈 값은 버리고 중복은 제거한다(mask_secrets 는 빈 문자열을 무시하지만 목록을 깨끗이 유지).
    """
    values = [token, str(repo_root), str(Path.home())]
    values += [
        v for k, v in env.items() if len(v) >= _SECRET_MIN_LEN and k not in _SECRET_SKIP_KEYS
    ]
    return list(dict.fromkeys(v for v in values if v))


def parse_allowed(raw: str) -> frozenset[int]:
    ids: set[int] = set()
    for tok in raw.split(","):
        tok = tok.strip()
        if tok:
            try:
                ids.add(int(tok))
            except ValueError:
                log.warning("허용목록에 숫자가 아닌 값 무시")
    return frozenset(ids)


def find_repo_root(start: Path) -> Path:
    """.git 이 있는 상위 폴더(모노레포 루트)를 찾는다."""
    for p in (start, *start.parents):
        if (p / ".git").exists():
            return p
    return start


# 모노레포 루트 — 여기서 한 번만 찾아 마스킹·SNS 수집함 경로가 같은 기준을 쓰게 한다(find_repo_root
# 는 함수 정의 뒤라야 호출 가능해 상단 경로 상수 블록이 아니라 여기에 둔다).
REPO_ROOT = find_repo_root(PROJECT_DIR)


def load_notify_state(path: Path, today: str) -> set[tuple[str, str]]:
    """notify_state.json → 오늘 날짜의 fired 집합(지난 날짜는 정리).

    형식: {"fired": [["id","YYYY-MM-DD"], ...]}. 파일 없음·손상은 빈 set(load_env 로더와 동일).
    옛 파일의 `snooze` 키는 읽지 않고 버린다 — 있어도 죽지 않는다.
    """
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):  # ValueError = JSONDecodeError · UnicodeDecodeError(비-UTF8)
        return set()
    fired: set[tuple[str, str]] = set()
    entries = raw.get("fired") if isinstance(raw, dict) else None
    if isinstance(entries, list):
        for entry in entries:
            if (
                isinstance(entry, list)
                and len(entry) == 2
                and isinstance(entry[0], str)
                and entry[1] == today
            ):
                fired.add((entry[0], entry[1]))
    return fired


def save_notify_state(path: Path, fired: set[tuple[str, str]]) -> None:
    """fired 를 원자적으로 영속(임시파일 write→replace)."""
    payload = {"fired": [[i, d] for i, d in sorted(fired)]}
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


SYSTEM_NOTICE_ROLE = "봇상태"  # 시스템 소식(기동·재연결·로그인 만료·다이제스트 포기)의 역할 채널


def post_system_notice(adapter: Adapter, text: str) -> bool:
    """시스템 소식을 #봇상태 로 1회 보낸다. 미매핑·전송 실패는 로그만 남기고 False(안 죽는다)."""
    channel = adapter.role_channel(SYSTEM_NOTICE_ROLE)
    if channel is None:
        log.warning("#%s 채널 미매핑 — 시스템 소식 스킵: %.60s", SYSTEM_NOTICE_ROLE, text)
        return False
    return adapter.send(channel, text) is not None


def resolve_notify_channel(adapter: Adapter, item: dict[str, Any]) -> tuple[int | None, str]:
    """알림 항목 → (발송 channelID, 표시명). `channel`(역할) 명시면 그 채널, 아니면 #봇상태.

    항목의 `project` 키는 읽지 않는다 — 프로젝트 채널이 없어져 무시되고(옛 notify.json 에 남아
    있어도 무해) `channel` 이 없으면 #봇상태 로 간다.
    """
    role = item.get("channel")
    if isinstance(role, str) and role:
        return adapter.role_channel(role), f"#{role}"
    return adapter.role_channel(SYSTEM_NOTICE_ROLE), f"#{SYSTEM_NOTICE_ROLE}"


def dispatch_notifications(
    adapter: Adapter,
    items: list[dict[str, Any]] | None = None,
) -> None:
    """주기 틱(≤NOTIFY_TICK_SEC) 호출 — 발송할 알림이 있으면 텍스트 한 줄로 발송한다.

    items=None(운영 기본)이면 매 틱 load_schedules(SCHEDULES_FILE)로 파일을 다시 읽는다(핫리로드).
    items 인자는 테스트가 스케줄을 직접 주입하는 seam. ponytail: 캐시·파일감시 불필요.

    due 판정은 하루 1회(`notify_fired` 의 (id, 날짜)). 상태 조회·변이는 _notify_lock 아래에서
    원자적으로(타이머 스레드↔워커 경합 방지), 실제 전송은 락 밖에서 한다.
    발송 타겟: resolve_notify_channel 이 정한 채널 1곳에 1회 send. 그 채널마저 미매핑이면 그 항목만
    스킵한다. id 가 DIGEST_RUNNERS 에 있는 항목은 **러너로** 간다(⏰ 텍스트 알림으로 새지 않는다).
    """
    if items is None:
        items = load_schedules(SCHEDULES_FILE)
    # `enabled: false` = **일시 정지**(삭제 아님). 키가 없으면 활성 — **명시적 false 만** 건너뛴다.
    items = [it for it in items if it.get("enabled") is not False]
    now = datetime.now(_KST)
    today = now.date().isoformat()
    with _notify_lock:
        # 날짜 경과분 정리(전역 재바인딩 회피 위해 메서드 호출).
        notify_fired.difference_update({k for k in notify_fired if k[1] != today})
        targets = due_notifications(items, now, notify_fired)
        if not targets:
            return
        # 전송 전 상태를 먼저 확정(동시 틱 재발송 방지) — 실제 전송은 락 밖.
        outgoing: list[tuple[str, dict[str, Any]]] = []
        for it in targets:
            item_id = it.get("id")
            if not isinstance(item_id, str) or not item_id:
                continue
            outgoing.append((item_id, it))
            notify_fired.add((item_id, today))
        save_notify_state(NOTIFY_STATE_FILE, notify_fired)
    for item_id, it in outgoing:
        channel, target = resolve_notify_channel(adapter, it)
        if channel is None:
            # ponytail: 자동생성 성공 시 역할 채널은 항상 있다. 없으면 degraded — 그 건만 스킵.
            log.warning("%s 채널 미매핑 — 알림 %s 발송 스킵", target, item_id)
            if item_id in DIGEST_RUNNERS:
                # 다이제스트는 하루 1회뿐이라 여기서 그냥 스킵하면 워커를 안 타 그날치가 재시도
                # 없이 날아간다. 봇 기동 직후 첫 틱이 on_ready(채널 자동생성) 전일 수 있어 현실적
                # → fired 를 풀어 다음 틱이 다시 잡게. 영구 미매핑이면 상한에서 멈춘다(공용 헬퍼).
                # (일반 알림은 fired 유지 — 시각 창이 지나면 어차피 안 잡힌다.)
                _revert_digest_fired(item_id, today, "채널 미매핑")
            continue
        if item_id in DIGEST_RUNNERS:
            # 수집·판정이 1~2분 걸려 타이머 스레드를 막으면 다른 알림이 밀린다 → 별도 데몬 스레드.
            _start_digest(adapter, channel, item_id, today)
            continue
        # ⚠️ try/except 로 감싸지 마라 — 계약상 예외를 던지지 않는다(§3.3: 실패는 로그+None).
        # 되돌리지(fired 해제) 않는 이유는 위 미매핑 경로와 같다. 그래서 로그만 남긴다.
        if adapter.send(channel, notify_text(it)) is None:  # 역할 채널 1회
            log.warning("%s 알림 %s 발송 실패 — 그날치 유실", target, item_id)


# ══════════════════════════════════════════════════════════════════════════
# 세션 1회 러너 배선 — 미국주식 다이제스트 · 스포티파이 월 1회 (알림 문구 대신 파이프라인으로 간다)
# ══════════════════════════════════════════════════════════════════════════
# ADR-003 불변식: 헤드리스 claude 에 네트워크 도구를 주지 않는다. 외부 데이터는 **브리지가
# urllib 로 선조회해 프롬프트에 텍스트 주입**(방식 B).
US_DIGEST_NOTIFY_ID = "us-digest"  # 미국주식 다이제스트(#미국주식) — 배선 공용, 러너만 다르다
# 다이제스트 id → **러너 함수명**. 알림 문구 대신 파이프라인으로 가는 항목의 유일한 정본이며
# dispatch_notifications·_run_digest 가 같은 이 맵을 본다(분기가 두 곳으로 갈라지지 않게).
# 값이 함수 객체가 아니라 **이름**인 이유: ① 러너가 아래에 정의돼 전방참조가 되고
# ② 테스트가 `monkeypatch.setattr(bridge, "run_us_digest", …)` 로 갈아끼우는데
# 객체를 잡아두면 그 교체가 안 먹는다(늦은 바인딩이 필요).
# 월 1회 스포티파이 주간차트 담기(#playlist). 다이제스트는 아니지만 «세션마다 후보 → 러너가
# 판정» 이라는 배선이 똑같아 같은 맵을 탄다(스케줄러를 새로 들이지 않는다).
SPOTIFY_NOTIFY_ID = "spotify-monthly"
# 매일 정오 #SNS정보 → 옵시디언 수집함(run_sns_inbox). 역시 «시각 창 + 러너» 배선이 같다.
SNS_NOTIFY_ID = "sns-inbox"
DIGEST_RUNNERS: dict[str, str] = {
    US_DIGEST_NOTIFY_ID: "run_us_digest",
    SPOTIFY_NOTIFY_ID: "run_spotify_monthly",
    SNS_NOTIFY_ID: "run_sns_inbox",
}
DIGEST_MAX_ATTEMPTS = 3  # 하루 실패-되돌림 상한(종일 실패 시 25초마다 재시도하지 않게)
# 도구 0개 티어 — 미국주식 LLM 이 뉴스만 요약하는 날(실적 스킬 창 밖)에 쓴다. cwd 가 레포 밖
# 샌드박스라도 외부 텍스트가 프롬프트에 들어오므로, 접근 경로 자체를 없애는 쪽(도구 0개)을 택한다.
# 빈 목록의 argv 표현은 claude_tool_args 참조(`--allowedTools` 빈 목록은 CLI 가 죽는다).
# **Bash 는 앞으로도 한 항목도 넣지 않는다**: `--allowedTools` 의 Bash 접두 매칭은 `;`·`&&`·`|`
# 체이닝을 못 막아 `git status; <임의명령>` 이 통과한다(2026-07-23 `Bash(curl …)` 반려와 같은 잣대).
DIGEST_TOOLS: list[str] = []
# 미국주식 다이제스트는 실적 스킬 창 안에서만 **`Skill` 1개**를 연다(ADR-004).
# ADR-003 불변식은 유지된다: Skill 은 네트워크 도구가 아니고, 파일·셸·git·웹 도구는 여전히 0개다.
# 스킬 탐색은 **cwd 기준**이라 US_DIGEST_SANDBOX_DIR 에 심은 것만 걸린다(개발자 세션엔 안 딸려간다).
US_DIGEST_TOOLS: list[str] = ["Skill"]
# 미국주식 전용 샌드박스 — 레포 밖 임시 폴더. 레포 루트를 cwd 로 쓰면 루트 CLAUDE.md 가 자동
# 로드되고 SessionStart 훅이 발동해 개발자의 2차 인증 잠금해제 마커를 지운다.
US_DIGEST_SANDBOX_DIR = Path(tempfile.gettempdir()) / "claude_bridge_us_digest_sandbox"
# 외부 조회는 **고정 host(kworb.net = 'ㅁ스포티파이' 의 차트 미러) + 경로**만 받아 조립한다
# (전체 URL 인자 금지 — SSRF 차단). GET 고정·타임아웃·실패는 조용히 스킵.
KWORB_HOST = "kworb.net"
_DIGEST_TIMEOUT = 8  # 초
_DIGEST_MAXBYTES = 300_000  # 응답 읽기 상한(거대 응답 방어)
_DIGEST_UA = "claude-bridge-digest"
# 제어문자·ANSI 이스케이프·비가시 유니코드 제거(AESI 방어). 사람 눈엔 안 보이는데 모델은 읽는
# 문자로 지시를 심는 공격을 **프롬프트 주입 전에** 끊는다. 보존은 `\t`(\x09)·`\n`(\x0a) 둘뿐 —
# `\r`(\x0d)도 제거한다(한 줄 필드에서 커서를 되돌려 앞 내용을 덮는 표시 위조 벡터).
_CTRL_RE = re.compile(
    r"\x1b\[[0-?]*[ -/]*[@-~]"  # CSI(ANSI) 시퀀스
    r"|\x1b[@-Z\\-_]"  # 그 외 이스케이프 시퀀스
    r"|[\x00-\x08\x0b-\x1f\x7f-\x9f]"  # C0/C1 제어문자(\t=\x09·\n=\x0a 만 제외)
    r"|[\u00ad\u200b-\u200f\u2060-\u2064\u202a-\u202e\u2066-\u2069\ufeff]"  # 폭0·bidi·BOM
    r"|[\ufe00-\ufe0f\U000e0100-\U000e01ef]"  # variation selector(1~256)
    r"|[\U000e0000-\U000e007f]"  # 유니코드 태그(보이지 않는 지시 삽입 벡터)
)


def strip_control(text: str) -> str:
    """외부 텍스트(README·HN 제목·설명)의 안 보이는 제어문자 제거. 프롬프트 주입 전 필수(순수)."""
    return _CTRL_RE.sub("", text)


def strip_control_line(text: str) -> str:
    """**한 줄 필드**(설명·HN 제목·URL·백로그 항목)용 — 제어문자 제거 + 공백 접기(순수).

    desc·title·url 은 프롬프트/백로그에서 한 줄로 렌더된다. 내부 개행이 살아남으면 외부 문자열
    하나로 가짜 `[출력 계약]` 섹션을 끼워 넣어 프롬프트 구조를 위조할 수 있다 → 전부 한 칸 공백
    으로 접는다. README 발췌(digest_excerpt)는 가독성상 개행을 살려야 하므로 여기에 태우지 않는다.
    """
    return re.sub(r"\s+", " ", strip_control(text)).strip()


def _digest_get(path: str, *, timeout: float = _DIGEST_TIMEOUT) -> bytes | None:
    """kworb.net 에 GET 1회 → 본문 bytes. 경로가 아니거나 실패면 None(조용히 스킵 — 부수 기능).

    SSRF 차단: 전체 URL 을 받지 않고 고정 host 에 경로/쿼리만 조립한다.
    리다이렉트는 추종하지 않는다(_NOREDIRECT_OPENER — 고정 host 밖으로 새는 경로를 원천 차단,
    3xx 는 HTTPError 로 승격돼 아래 폴백으로 떨어진다).
    """
    if not path.startswith("/"):
        return None
    req = urllib.request.Request(
        f"https://{KWORB_HOST}{path}",
        method="GET",  # GET 고정
        headers={"User-Agent": _DIGEST_UA, "Accept-Encoding": "identity"},
    )
    try:
        with _NOREDIRECT_OPENER.open(req, timeout=timeout) as resp:
            body: bytes = resp.read(_DIGEST_MAXBYTES)
    except Exception as exc:
        # 방어적 광범위 캐치 — 어떤 예외도 데몬 스레드로 새지 않게.
        # 403(rate limit)·429·404·타임아웃 전부 여기서 조용히 흡수한다.
        log.info("다이제스트 조회 실패 %s%.60s (%s)", KWORB_HOST, path, type(exc).__name__)
        return None
    return body


def fetch_digest_text(path: str) -> str:
    """kworb.net GET → 제어문자 제거한 텍스트. 실패는 ""."""
    raw = _digest_get(path)
    return strip_control(raw.decode("utf-8", "replace")) if raw is not None else ""


def _revert_digest_fired(item_id: str, today: str, reason: str) -> bool:
    """다이제스트 fired 선기록 되돌림 — 하루 DIGEST_MAX_ATTEMPTS 회까지만.

    되돌림 지점이 둘이다: 워커(_run_digest, 파이프라인 실패)와 틱(dispatch_notifications,
    역할 채널 미매핑). **상한 카운터가 한쪽에만 있으면 다른 쪽은 25초마다 영원히
    재시도**하며 WARNING 을 하루 수천 줄 쌓는다 → 카운팅·되돌림을 여기 한 곳으로 모은다.
    상한 도달 후엔 fired 를 유지해 그날은 조용히 포기한다(봇 기동 직후 on_ready 전 1~2틱의
    자기치유는 상한 안이라 그대로 산다).
    **예산은 다이제스트 id 별로 따로 센다** — 세션 항목 둘이 같은 틱에 함께 도는데 예산을
    공유하면 한쪽 장애가 다른 쪽 그날치를 통째로 삼킨다(로그에도 어느 쪽인지 남긴다).

    🔴 **포기했으면 True 를 돌린다.** 호출부가 그때 사람에게 알린다 — 로그만 남기면 「안 온 것」은
    아무도 모른다. 「그날치를 통째로 버렸다」는 로그 등급이 경고가 아니라 **오류**다.
    """
    with _notify_lock:
        key = (item_id, today)
        tries = _digest_attempts.get(key, 0) + 1
        for stale in [k for k in _digest_attempts if k[1] != today]:
            del _digest_attempts[stale]  # 어제 것만 정리(clear 면 형제 카운터까지 날아간다)
        _digest_attempts[key] = tries
        if tries >= DIGEST_MAX_ATTEMPTS:
            log.error(
                "다이제스트 %s %s %d회 — 오늘은 재시도 중단(그날치 유실)", item_id, reason, tries
            )
            # 통지는 **상한을 넘는 그 순간 한 번만** — 중복 호출로 이 자리에 다시 와도
            # 같은 말을 되풀이하지 않는다(로그는 매번 남는다).
            return tries == DIGEST_MAX_ATTEMPTS
        notify_fired.discard(key)
        save_notify_state(NOTIFY_STATE_FILE, notify_fired)
        log.info(
            "다이제스트 %s %s %d/%d — 다음 틱 재시도", item_id, reason, tries, DIGEST_MAX_ATTEMPTS
        )
        return False


def _start_digest(adapter: Adapter, channel_id: int, item_id: str, today: str) -> None:
    """다이제스트를 별도 데몬 스레드로 띄운다 — 수집·판정 1~2분이 타이머 스레드를 막지 않게."""

    def run() -> None:
        try:
            _run_digest(adapter, channel_id, item_id, today)
        finally:
            _busy_add(-1)

    _start_busy_thread(run, item_id)  # 스레드 이름 = 다이제스트 id(로그에서 어느 쪽이 도는지 구분)


# 포기 알림에 쓰는 이름(목적격 «를» 로 이어지는 형태). 맵에 없는 id 는 id 자체를 쓴다.
_DIGEST_GIVEUP_LABEL = {
    US_DIGEST_NOTIFY_ID: "마이크론 카드",
    SPOTIFY_NOTIFY_ID: "스포티파이 월간 차트",
    SNS_NOTIFY_ID: "SNS정보 수집",
}


def digest_giveup_text(item_id: str, now: datetime | None = None) -> str:
    """다이제스트가 하루 상한만큼 실패해 그날치를 버렸을 때의 #봇상태 문구. 순수."""
    label = _DIGEST_GIVEUP_LABEL.get(item_id, f"`{item_id}`")
    return f"{notice_stamp(now)} ⛔ {label} 생성 실패\n→ claude-bridge/logs/bridge.log"


def _run_digest(adapter: Adapter, channel_id: int, item_id: str, today: str) -> None:
    """다이제스트 실행 + **실패 시 fired 되돌림**(그날치 영구 유실 방지).

    fired 선기록은 그대로 둔다 — 25초 틱이 같은 다이제스트를 겹쳐 돌리는 것을 반드시 막아야
    하기 때문(수집·판정이 분 단위라 겹치면 API 낭비·중복 게시). 대신 파이프라인이 실패하면
    _revert_digest_fired 로 discard 해 다음 틱이 다시 잡게 한다(상한은 그 함수가 건다).
    실행할 러너는 DIGEST_RUNNERS 에서 **이름으로** 찾는다(늦은 바인딩 — 그 상수 주석 참조).
    """
    try:
        runner = globals()[DIGEST_RUNNERS[item_id]]
        with _working():
            posted = runner(adapter, channel_id, today)
    except Exception:
        # 데몬 스레드가 조용히 죽지 않게 — 실패로 취급해 되돌린다.
        # ⚠️ **역추적을 함께 남긴다** — 다이제스트는 스레드 안이라 이 로그가 유일한 증거다
        # (상위로 전파되지 않고, 타입만 찍으면 어느 단계에서 터졌는지 알 길이 없다).
        log.exception("다이제스트 예외 (%s)", item_id)
        posted = False
    if not posted and _revert_digest_fired(item_id, today, "실패"):
        # 🔴 그날치를 버렸다 → **사람에게 말한다**(「안 온 것」은 눈에 안 띈다).
        # 도배 걱정은 없다: 상한에 닿으면 fired 를 유지하므로 이 자리는 **하루 1회**만 온다.
        # 채널은 #봇상태 — 다이제스트 채널을 «안 온 것» 알림으로 더럽히지 않는다.
        post_system_notice(adapter, digest_giveup_text(item_id))


# ══════════════════════════════════════════════════════════════════════════
# 📈 마이크론 다이제스트 (21:30 KST 시각 발화 · #마이크론) — ① 오늘 월~금 / ② 분석 일요일 단독
# ══════════════════════════════════════════════════════════════════════════
# 수집·계산·포매팅은 전부 us_digest 모듈이 한다(bridge 는 배선만). 이 러너는
# **claude 를 직접 부르지 않는다** — 판정이 아니라 재료 제공이다(뉴스 요약 LLM 은 us_digest 안).
# 요일 판정(일요일이면 ② 분석 단독)도 us_digest 안에서 한다 — `notify.json` 의 `us-digest` 항목
# 하나로 처리한다(⚠️ 새 id 를 추가하지 마라: 구동 중인 옛 봇이 일반 알림으로 발송한다).
def run_us_digest(adapter: Adapter, channel_id: int, today: str) -> bool:
    """마이크론 카드 게시(월~금 ① · 일요일 ②). 반환 = 게시 성공 여부(False 면 재시도).

    카드를 못 만든 경우(us_digest 가 None = MU 시세 실패)와 **첫 메시지** 게시 실패를
    False 로 낸다 — 보유 종목 시세가 빠진 카드는 낼 이유가 없고, 죽은 소스 하나 때문에 그날치를
    포기하지도 않는다(블록 단위 부분 실패는 us_digest 안에서 `조회 실패`로 흡수된다).
    ⚠️ 그 뒤 메시지 실패는 False 로 내지 않는다 — 앞 메시지는 이미 올라갔고, False 면 다음 틱이
    한 번 더 올린다(중복). 실패는 경고 로그로 남기고 남은 메시지를 접는다.
    """
    card = us_digest.build_us_digest(today)
    if card is None:
        return False
    # 카드 스펙을 **일반 메시지 마크다운**으로 펴서 text 로 보낸다(card 인자 없음) — 어댑터는 이
    # 경로에서 임베드로 감싸지 않고(`#` 로 시작하면 상태 헤더가 아니다) 서식도 건드리지 않는다.
    # 카드 하나 = 메시지 하나가 기본이고, 2,000자를 넘으면 필드 경계에서 여러 메시지가 된다.
    # ⚠️ send 를 try/except 로 감싸지 마라 — **계약상 예외를 던지지 않는다**(§3.3: 플랫폼 오류는
    # 어댑터가 삼키고 로그+None). 감싸면 그 except 가 죽은 코드가 되고 실패가 True 로 나가
    # fired 가 유지된다 → 그날 카드 0장에 재시도도 에러도 없다(봇 기동 직후 이벤트루프 미준비
    # 틱에서 실제로 난다). **성공 판정은 반환값으로만.**
    # **첫 메시지**만 False(재시도)로 낸다 — 그 뒤 실패에서 False 면 이미 올라간 앞 메시지가 다음
    # 틱에 한 번 더 올라간다(중복). 나머지 실패는 경고만 남기고 남은 메시지를 접는다.
    for part, message in enumerate(us_digest.card_messages(card), start=1):
        if adapter.send(channel_id, message, None) is not None:
            continue
        if part == 1:
            log.warning("마이크론 다이제스트 게시 실패 — 되돌려 다음 틱 재시도")
            return False
        log.warning("마이크론 다이제스트 %d번째 메시지 게시 실패 — 건너뜀", part)
        break
    log.info("마이크론 다이제스트 게시 완료")
    return True


# ── 🧪 드라이런(`python bridge.py --us-digest-dry-run`) ────────────────────────
def us_digest_dry_run(weekly: bool | None = None) -> int:
    """마이크론 다이제스트를 **게시 없이** 1회 조립해 stdout 으로 찍는다. 반환 = 종료코드.

    `weekly=True`(`--weekly`)면 요일과 상관없이 ② 분석(단독)을 찍는다. 기본은 오늘 요일대로
    (일요일 ② · 그 밖 ①).
    라이브와 **같은 함수**(us_digest.build_us_digest · card_messages)를 쓴다 — 메시지 사이
    구분선(`=== 메시지 …`)만 빼면 **실제로 보낼 마크다운 원문 그대로**다. 상태 격리가
    필요 없다 — seen·기각 같은 소모성 상태가 없고 유일한 쓰기인 SEC 요약 캐시는 하루 1회
    재조회를 아끼는 것이라 라이브에도 이롭다. 그래서 출력 파일도 남기지 않는다(표준출력이면 충분).
    """
    today = datetime.now(_KST).date().isoformat()
    started = time.monotonic()
    card = us_digest.build_us_digest(today, weekly=weekly)
    took = time.monotonic() - started
    if card is None:
        print(f"(카드 없음 — {us_digest.TICKER} 시세 조회 실패. 수집 {took:.1f}초)")
        return 1
    messages = us_digest.card_messages(card)
    for number, message in enumerate(messages, start=1):
        print(f"=== 메시지 {number}/{len(messages)} · {len(message):,}자 ===")
        print(message)
    print(f"[소요]     {took:.1f}초")
    return 0


# ── 단일 인스턴스 락(pidfile) ───────────────────────────────────────────────
def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        )
        # D3: PID 생존뿐 아니라 이미지명이 python 계열인지 확인 — 재부팅 후 stale pid 를
        # 무관 프로세스가 재사용하면 락 오탐으로 브리지가 조용히 안 뜨는 것을 막는다.
        line = r.stdout.strip().lower()
        return str(pid) in line and "python" in line
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_lock(pidfile: Path) -> bool:
    """다른 인스턴스가 살아있으면 False(409 방지)."""
    if pidfile.exists():
        try:
            old = int(pidfile.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            old = 0
        if old and old != os.getpid() and _pid_alive(old):
            return False
    pidfile.write_text(str(os.getpid()), encoding="utf-8")
    return True


# ══════════════════════════════════════════════════════════════════════════
# claude 실행
# ══════════════════════════════════════════════════════════════════════════
def _kill_tree(proc: subprocess.Popen[str]) -> None:
    # D1: Windows 에서는 부모가 살아있을 때 `taskkill /T` 로 자식 트리를 먼저 열거·종료해야
    # 손자 프로세스까지 정리된다(부모를 먼저 죽이면 트리를 열거 못 해 손자 잔존). 그 다음 kill 폴백.
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
            capture_output=True,
            check=False,
        )
    with contextlib.suppress(OSError):
        proc.kill()


def _warn_context_leak(cwd: Path) -> None:
    """도구 0개 티어 cwd 에 **훅 차단이 못 막는 컨텍스트**가 생기면 경고만 한다(막지는 않는다).

    `--settings` 는 훅만 끈다 — 상위 `CLAUDE.md` 자동 발견과 auto-memory 는 그대로 살아 있고
    (카나리 실측), 게다가 settings **키가 오타·개명이면 CLI 는 rc=0·경고 0 으로 넘어간다**.
    즉 이 티어는 깨져도 조용하다 → 값싼 관측점 하나를 런타임에 남긴다. 판정은 계속 돌아야 하므로
    차단하지 않는다. ponytail: memory 키는 cwd 문자열 치환 추정(어긋나면 경고를 못 낼 뿐).
    """
    key = re.sub(r"[^A-Za-z0-9]", "-", str(cwd))  # cwd → `~/.claude/projects/<키>` 치환 규칙
    memory = Path.home() / ".claude" / "projects" / key / "memory"
    leaks = [str(p / "CLAUDE.md") for p in (cwd, *cwd.parents) if (p / "CLAUDE.md").is_file()]
    leaks += [str(memory)] if any(memory.glob("*")) else []
    if leaks:
        log.warning("도구 0개 티어 컨텍스트 유입 경로 — 훅 차단이 못 막는다: %s", leaks[:3])


def claude_tool_args(tools: list[str]) -> list[str]:
    """도구 화이트리스트 → claude argv 조각(순수). **빈 목록 = 도구 0개**.

    빈 목록을 그대로 `--allowedTools` 에 붙이면 안 된다 — CLI 가 `option '--allowedTools
    <tools...>' argument missing` 으로 **즉시 죽는다**(2026-07-27 실측). "빈 리스트 = 제한 없음"
    으로 뒤집히지는 않지만, 실행 자체가 안 되므로 0개는 다른 플래그로 표현해야 한다.
    · `--tools ""` = 내장 도구 전부 끔(CLI 도움말의 명시 계약). 실측: 캐너리 파일 Read 요청에
      NOTOOL 응답·num_turns 1(도구 호출 0).
    · `--strict-mcp-config` = MCP 서버 무로딩. `--tools ""` **만으로는 MCP 도구가 그대로 노출**
      된다(실측: `mcp__serena__find_symbol`·`mcp__git__git_show` 호출 시도 → 권한 거부로 막히긴
      하나 턴·토큰을 태우고, 설정이 그 서버를 allow 로 두면 그대로 뚫린다).

    **`--strict-mcp-config` 는 전 티어 공통**(2026-07-27): `--allowedTools` 는 *권한* 목록일 뿐
    *가용성* 목록이 아니다 — `WebSearch` 1개 티어로 띄워도 `system/init` 이 도구 75개를
    보고했고(내장 30 + MCP 45) 그 안에 `git_commit`·`git_reset`·`chrome-devtools__navigate_page`
    ·`KakaotalkChat-MemoChat`(외부 발신)이 그대로 있었다. 실제 차단은 권한 엔진이 하는데
    `~/.claude/settings.json` 과 워크스페이스 `settings.local.json` 이 **둘 다
    `defaultMode: bypassPermissions`** 라, 그것을 덮는 건 run_claude 의 `--permission-mode
    default` **한 줄뿐**이었다. 이 플래그가 MCP 쪽 가용성을 아예 없애 두 번째 축을 만든다
    (내장 도구는 `--tools ""` 로만 없앨 수 있어 비-빈 티어에서는 여전히 권한 계층 의존).
    어느 티어도 MCP 를 쓰지 않는다.

    **순서가 안전장치다(M-1)**: `--strict-mcp-config` 를 `--tools ""` **앞**에 둔다. 뒤에 두면
    빈 문자열이 어떤 이유로든 소실될 때 argv 가 `--tools --strict-mcp-config` 가 되고, commander
    가 뒤 플래그를 `--tools` 의 **값으로 삼켜** MCP 45개가 에러도 로그도 없이 열린다(fail-open,
    실측). 앞에 두면 같은 소실이 `rc=1 argument missing` 으로 죽는다(fail-closed).

    """
    # **도구 0개 티어에만 훅 차단**(2026-08-02 라이브 결함). cwd 를 레포 밖으로 뺀 것은 *프로젝트*
    # 훅·CLAUDE.md 만 막는다 — **사용자 전역(`~/.claude`)·플러그인 훅은 cwd 무관**이라 그대로
    # 통과한다(실측: 플러그인 SessionStart 훅이 statusLine 추가를 요청하는 문장이 판정 컨텍스트에
    # 주입돼 검토 보고서에 그대로 언급됐다).
    # ⚠️ `--safe-mode` 를 쓰지 마라 — CLI 2.1.138 에 없는 플래그라 argv 파싱 단계에서
    # `unknown option` 으로 즉사한다(실측). 대신 쓰는 `--settings '{"disableAllHooks": true}'` 는
    # 커버리지가 좁다(훅만 끔). 이 티어엔 **지금은** 충분하다 — 도구 0개라 Skill 호출이
    # 불가해서다(실측).
    # ⚠️ 나머지 둘은 **이 플래그가 막아주는 것이 아니다**(카나리 실측으로 반증):
    #   · 상위 `CLAUDE.md` 자동 발견은 **살아 있다** — 샌드박스가 temp 라 조상에 홈 디렉터리가 있고,
    #     거기 파일을 심으면 판정 모델이 그대로 복창했다. 지금 안 붙는 건 그 경로에 파일이 없어서다.
    #   · auto-memory 도 **꺼지지 않는다** — cwd 로 `projects/<키>/memory` 키가 갈릴 뿐이라
    #     그 스코프에 파일이 생기면 붙는다(빈 상태라 안 붙을 뿐).
    #   → 하나라도 생기면 판정 컨텍스트에 외부 텍스트가 실린다. _warn_context_leak 이 경고.
    # ⚠️ `--bare` 로 바꾸지 마라: OAuth·keychain 을 안 읽어 구독 인증이 끊긴다(실측).
    # ⚠️ 비-빈 티어에 확대하지 마라(ADR-004) — 스킬 티어(US_DIGEST_TOOLS)는 훅 차단 대상이 아니다.
    # 순서: 맨 앞에 둬 `--tools ""` 의 fail-closed 순서 계약(strict 가 바로 앞)을 건드리지 않는다.
    # ※ 값 있는 플래그가 된 뒤로는 값이 소실돼도 fail-open 이 아니다 — 실측상 뒤 플래그를 값으로
    # 삼켜 `Settings file not found: --strict-mcp-config` 로 **시끄럽게 죽는다**(도구는 안 열린다).
    return [
        *(["--settings", '{"disableAllHooks": true}'] if not tools else []),
        "--strict-mcp-config",
        *(["--allowedTools", *tools] if tools else ["--tools", ""]),
    ]


# 🔐 Claude 로그인 만료 감지 — 모든 claude 실행은 run_claude 를 지나므로 그 결과 한 곳에서 본다.
# ⚠️ 패턴은 **잠정치**다(실제 만료 때 CLI 가 내는 문구를 아직 실측하지 못했다). 오류 결과(`is_error`)
# 에서만 보며, 만료 때 나는 문구를 확인하면 이 상수 하나만 고친다. `401` 은 단어 경계로만 맞춘다.
_LOGIN_EXPIRED_RE = re.compile(
    r"/login|not logged in|invalid api key|oauth|authentication|\b401\b", re.IGNORECASE
)
LOGIN_EXPIRED_TEXT = "🔐 Claude 로그인 필요"
# 알림 배선(main 이 채운다) — 보냈으면 True. run_claude 는 adapter 를 모르므로 훅으로 받는다.
login_alert: Callable[[], bool] | None = None
_login_alert_day = ""  # 마지막으로 알린 날(KST) — 하루 1회


def set_login_alert(hook: Callable[[], bool] | None) -> None:
    """로그인 만료 알림 훅을 건다(main 이 어댑터를 쥐고 배선, 테스트는 None 으로 해제)."""
    global login_alert
    login_alert = hook


def looks_like_login_expired(data: dict[str, Any]) -> bool:
    """claude 결과가 «로그인 만료»로 보이는가. 오류 결과일 때만(타임아웃 등 일반 실패는 아니다)."""
    if not data.get("is_error"):
        return False
    return _LOGIN_EXPIRED_RE.search(str(data.get("result", ""))) is not None


def _report_login_expired(data: dict[str, Any]) -> None:
    """만료로 보이면 #봇상태 에 하루 1회 알린다. 전송 실패면 같은 날 다시 시도할 수 있게 푼다."""
    global _login_alert_day
    hook = login_alert
    if hook is None or not looks_like_login_expired(data):
        return
    today = datetime.now(_KST).date().isoformat()
    with _notify_lock:
        if _login_alert_day == today:
            return
        _login_alert_day = today
    try:
        sent = hook()
    except Exception as exc:  # 알림 실패가 claude 실행 결과 반환을 막으면 안 된다
        log.warning("로그인 만료 알림 실패: %s", type(exc).__name__)
        sent = False
    if not sent:
        with _notify_lock:
            _login_alert_day = ""


_P = ParamSpec("_P")


def _watch_login(fn: Callable[_P, dict[str, Any]]) -> Callable[_P, dict[str, Any]]:
    """run_claude 결과를 _report_login_expired 에 흘려보내는 데코레이터(반환값은 그대로)."""

    @functools.wraps(fn)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> dict[str, Any]:
        with _working():  # 자동 재시작 «바쁨» 판정
            data = fn(*args, **kwargs)
        _report_login_expired(data)
        return data

    return wrapper


@_watch_login
def run_claude(
    claude_exe: str,
    project_path: str,
    task: str,
    timeout: int,
    on_event: Callable[[dict[str, Any]], None] | None = None,
    *,
    allowed_tools: list[str],
    system_prompt: str,
) -> dict[str, Any]:
    """claude -p 를 stream-json 으로 실행, NDJSON 이벤트를 증분 소비한다.

    on_event: 파싱된 이벤트 dict 마다 호출. 최종 `result` 이벤트를 그대로
    반환(`.result`·`.is_error`·`.total_cost_usd`). result 없이 끝나면
    is_error 폴백. 스트림이라 communicate(timeout=) 을 못 쓰므로 리더 데몬 스레드 +
    메인 deadline join 패턴을 쓴다(초과 시 `_kill_tree` 로 트리 정리).

    스트림 리더는 (D2) `result` 이벤트 저장 직후 break 한다 — MCP 손자 프로세스가 상속한
    stdout write 핸들을 붙잡아 EOF 가 안 와도 데드라인까지 대기하지 않는다(오타임아웃 방지).
    stderr 는 (D1) 별도 드레인 스레드가 실시간 배수해 파이프 버퍼 포화로 인한 자식 블록을
    막고, 마지막 N줄만 폴백 진단용으로 보관한다. 리더 종료 후엔 (D3) `_kill_tree` 로
    손자(MCP)까지 정리한 뒤 reap 한다.

    보안(C-1): 사용자 task 는 argv 에 두지 않고 **stdin 으로만** 전달한다. Windows 에서
    `shutil.which("claude")` 는 배치 shim(claude.CMD)으로 해석돼 argv 가 cmd.exe 재파싱을
    거치므로, task 를 인자로 넘기면 큰따옴표+`&` 로 명령 인젝션(RCE)이 가능하다
    (shell=False·리스트 인자로도 못 막음). argv 엔 정적·신뢰 플래그만 남긴다.
    """
    # 도구 목록은 호출부가 명시한다(기본값 없음) — 빈 목록 = 도구 0개 티어(DIGEST_TOOLS).
    # **Bash 는 어느 티어에도 넣지 않는다**(DIGEST_TOOLS 주석 참조).
    tools = allowed_tools
    if not tools:
        _warn_context_leak(Path(project_path))  # 훅 차단이 못 막는 유입 경로 관측(경고만)
    cmd = [
        claude_exe,
        "-p",
        "--output-format",
        "stream-json",  # 증분 이벤트(NDJSON) — -p 에서 --verbose 필수
        "--verbose",
        "--model",
        "opus",
        "--permission-mode",
        "default",
        "--append-system-prompt",
        system_prompt,
        *claude_tool_args(tools),
    ]
    # ponytail: Windows 프로세스 그룹으로 자식 트리까지 정리(타임아웃 시 taskkill /T).
    flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=project_path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=flags,
        )
    except OSError as e:
        return {"is_error": True, "result": f"claude 실행 불가: {type(e).__name__}"}

    result_box: dict[str, Any] = {}
    err_tail: deque[str] = deque(maxlen=40)  # D1: stderr 마지막 N줄만(폴백 진단용)

    def reader() -> None:
        stdin = proc.stdin
        stdout = proc.stdout
        if stdin is None or stdout is None:
            return
        # task 는 stdin 전용(C-1). write 후 close 해 claude 가 입력 종료를 인지하게 한다.
        with contextlib.suppress(OSError):
            stdin.write(task)
            stdin.close()
        for raw in stdout:  # NDJSON 한 줄 = 한 이벤트, 증분 소비
            line = raw.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue  # 깨진 줄은 skip·계속(브리지·작업 안 죽게)
            if not isinstance(event, dict):
                continue
            if on_event is not None:
                try:
                    on_event(event)
                except Exception as e:  # 진행표시 오류가 스트림 리더를 죽이지 않게(타입만)
                    log.warning("on_event 실패: %s", type(e).__name__)
            if event.get("type") == "result":
                # D2: result 저장 직후 break — 스트림상 result 뒤엔 유의미 이벤트가 없다.
                # MCP 손자가 stdout write fd 를 붙잡아 EOF 가 안 와도 데드라인까지
                # 대기하지 않게 여기서 끊는다(오타임아웃 방지).
                result_box["data"] = event
                break

    def drain() -> None:
        # D1: 실행 중 stderr 를 배수하지 않으면 파이프 버퍼 포화 → 자식 블록 → 거짓 타임아웃.
        # 드레인 스레드가 stderr 를 소유하고 마지막 N줄만 보관한다(폴백 시 진단 텍스트).
        stderr = proc.stderr
        if stderr is None:
            return
        with contextlib.suppress(OSError, ValueError):
            for raw in stderr:
                err_tail.append(raw.rstrip())

    t = threading.Thread(target=reader, daemon=True)
    te = threading.Thread(target=drain, daemon=True)
    t.start()
    te.start()
    t.join(timeout)
    if t.is_alive():
        # 전체 데드라인 초과 — 트리 정리 후 중단(D1: taskkill /T → kill).
        _kill_tree(proc)
        t.join(5)
        with contextlib.suppress(subprocess.TimeoutExpired, OSError):
            proc.wait(timeout=10)
        # D2 방어(두 겹): 타임아웃이라도 이미 result 를 캡처했으면 살려서 반환(오타임아웃 방지).
        data = result_box.get("data")
        if isinstance(data, dict):
            return data
        return {"is_error": True, "result": f"타임아웃({timeout}s) 초과 — 작업을 중단했습니다"}

    # 리더가 result break 또는 stdout EOF 로 종료 — D2/D3: 손자(MCP) 트리를 정리 후 reap.
    # (result 뒤엔 세션 끝이라 kill 안전; 이미 죽었으면 무해.)
    _kill_tree(proc)
    with contextlib.suppress(subprocess.TimeoutExpired, OSError):
        proc.wait(timeout=10)

    data = result_box.get("data")
    if isinstance(data, dict):
        return data
    # result 이벤트 없이 끝남(크래시·기동 실패 등) — stderr 드레인 버퍼로 폴백.
    te.join(2)  # 드레인이 마지막 줄까지 배수하도록 잠깐 대기(deque 동시변경 회피 겸).
    err = "\n".join(err_tail).strip()[-500:]
    return {"is_error": True, "result": err or f"claude 응답 없음(rc={proc.returncode})"}


# ── 코드 변경 자동 재시작 ────────────────────────────────────────────────
# «바쁨» = 이벤트 처리 중(handle_event) + 진행 중 claude 실행 + 백그라운드 스레드(다이제스트·SNS·
# 청소). 음악 재생은 바쁨이 아니라 is_idle 의 is_music_active 로 따로 본다(재생은 끝이 없어서).
_busy = 0
_busy_lock = threading.Lock()
_reload_requested = False  # main 이 종료 코드를 RELOAD_EXIT_CODE 로 바꾸는 신호


@contextlib.contextmanager
def _working() -> Iterator[None]:
    global _busy
    with _busy_lock:
        _busy += 1
    try:
        yield
    finally:
        with _busy_lock:
            _busy -= 1


def _busy_add(n: int) -> None:
    global _busy
    with _busy_lock:
        _busy += n


def _start_busy_thread(run: Callable[[], None], name: str) -> None:
    """«바쁨» 을 **start 전에** 올리고 데몬 스레드를 띄운다 — 감소는 run 의 finally 몫.

    스레드 안에서 올리면 start 와 첫 줄 사이에 자동 재시작 감시가 «한가함» 으로 보고 끊을 수 있다.
    """
    _busy_add(1)
    try:
        threading.Thread(target=run, name=name, daemon=True).start()
    except BaseException:
        _busy_add(-1)  # 못 띄웠으면 되돌린다(스레드가 없으니 finally 도 없다)
        raise


def code_snapshot(root: Path) -> dict[str, int]:
    """프로젝트 루트 *.py(tests 제외 — 하위 폴더는 glob 에 안 걸린다)의 mtime_ns."""
    snap: dict[str, int] = {}
    for p in root.glob("*.py"):
        with contextlib.suppress(OSError):  # 저장 도중 교체돼 사라진 파일은 다음 틱에 본다
            snap[p.name] = p.stat().st_mtime_ns
    return snap


class ReloadWatcher:
    """기동 시점 스냅샷과 달라진 뒤 settle 초 동안 더 안 바뀌면 check() 가 True."""

    def __init__(
        self,
        root: Path,
        settle: float = RELOAD_SETTLE_SEC,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._root = root
        self._settle = settle
        self._clock = clock
        self._base = code_snapshot(root)
        self._last = self._base
        self._since = 0.0

    def check(self) -> bool:
        cur = code_snapshot(self._root)
        if cur == self._base:
            return False  # 되돌려져 기동 때와 같아졌다 — 대기 해제
        if cur != self._last:
            self._last = cur
            self._since = self._clock()  # 또 바뀌었다 — 안정화 타이머 재시작
        return self._clock() - self._since >= self._settle


def is_idle(adapter: Adapter) -> bool:
    """진행 중 claude·다이제스트가 없고 음악 재생 중도 아니면 True."""
    if _busy:
        return False
    music = getattr(adapter, "is_music_active", None)  # 계약 밖 어댑터 훅(wait_ready 와 같은 방식)
    return not (callable(music) and music())


def reload_if_ready(adapter: Adapter, watcher: ReloadWatcher, marker: Path | None = None) -> bool:
    """변경 안정 + 한가하면 마커를 남기고 어댑터를 닫는다(→ poll 종료 → main 이 75 로 반환).

    Off 알림은 보내지 않는다(조용한 재시작). 닫았으면 True.
    """
    global _reload_requested
    if not watcher.check() or not is_idle(adapter):
        return False
    log.info("코드 변경 감지 — 한가함, 자동 재시작(exit %d)", RELOAD_EXIT_CODE)
    marker = marker or RELOAD_MARKER  # 기본값을 def 시점에 묶지 않는다(테스트 격리가 덮어쓴다)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("reload", encoding="utf-8")
    _reload_requested = True
    adapter.close()
    return True


def _reload_loop(adapter: Adapter, stop: threading.Event, watcher: ReloadWatcher) -> None:
    while not stop.wait(RELOAD_POLL_SEC):
        try:
            if reload_if_ready(adapter, watcher):
                return
        except Exception as e:  # 감시 오류로 스레드가 죽지 않게(타입만 기록)
            log.error("자동 재시작 감시 예외: %s", type(e).__name__)


def consume_reload_marker(marker: Path | None = None) -> bool:
    """마커가 있으면 지우고 True(= 직전 종료가 자동 재시작이었다)."""
    marker = marker or RELOAD_MARKER
    if not marker.exists():
        return False
    marker.unlink(missing_ok=True)
    return True


def _restart(adapter: Adapter) -> None:
    """재시작 명령: 어댑터 정리(close) → 프로세스 종료(exit 0). 런처/systemd 재기동.

    재기동 뒤의 «돌아왔다» 신호는 어댑터의 🟢 기동 알림(#봇상태)이 맡는다. close() 가
    Gateway/이벤트루프를 정리한다. 진행 중 claude 실행이 있어도 강제 종료 수용(개인용 자기수정
    루프 — 드레이닝 과설계 금지). 회신은 호출측이 exit 전에 이미 보냈다(멱등 close 라 main finally
    와 이중 안전).
    """
    log.info("재시작 요청 — 어댑터 정리 후 종료(exit 0)")
    post_system_notice(adapter, off_notice_text())  # 🔴 스스로 꺼질 때만 «꺼지기 전»에 보낼 수 있다
    adapter.close()
    sys.exit(0)


# ══════════════════════════════════════════════════════════════════════════
# 이벤트 처리 (통합 디스패처 handle_event + kind 별 헬퍼)
# ══════════════════════════════════════════════════════════════════════════
# §4.8 목업 CASE6(폰 실측 반영): 섹션 제목 `## `(디스코드 큰 헤더), 명령어는 제목 다음 줄
# `명령어 - …`, 부가 힌트는 `-# ` 서브텍스트(작은 회색)로 위계 분리. 한글 명령 주력·영어 별칭 병기.
HELP_TEXT = (
    "### 채널 청소 — ㅁ청소\n"
    "확인을 거친 뒤 이 채널의 메시지를 전부 지웁니다(되돌릴 수 없음).\n"
    "#SNS정보 에서는 🔗 링크청소(저장된 링크·명령·카드만) · 🧹 전체청소(링크 수집 뒤 전부)"
    " 버튼이 뜹니다.\n"
    "\n"
    "### 재시작 — ㅁ재시작\n"
    "브리지(봇)를 다시 켭니다. 코드 수정을 반영하거나 봇이 멈췄을 때 씁니다.\n"
    "\n"
    "### 음악 — ㅁ노래\n"
    "음성채널에 들어가 배경음악을 재생합니다. 정지 ㅁ정지 · 다음곡 ㅁ다음.\n"
    "ㅁ재생 <제목> 은 그 곡을 지금 틀고(목록에 없으면 유튜브에서 찾아 한 번 재생), "
    "ㅁ삭제 <제목> 은 재생목록에서 그 곡을 뺍니다.\n"
    "ㅁ목록 은 재생목록 전곡을 번호·제목으로 보여줍니다.\n"
    "ㅁ스포티파이 는 스포티파이 월간차트(글로벌·일본·한국) 상위 30곡씩을 한 번에 담습니다"
    "(약 7분 소요 — 끝나면 추가·중복·실패 곡수를 알려줍니다).\n"
    "ㅁ추가 <검색어> #N 은 검색 결과 N번째를 넣습니다(번호는 검색 순번). "
    "번호를 안 주면 방송무대·교차편집·직캠을 걸러 고릅니다."
)


def _handle_button(adapter: Adapter, event: Event) -> None:
    """인라인 버튼 탭 처리(청소 확인·SNS 판정). 화이트리스트 라우팅.

    보안: 허용목록 게이트는 handle_event 가 이 함수 진입 전에 통과시킨다. action/arg 는 어댑터가
    parse_callback 정확 매칭으로 정규화한 값(임의 실행 금지).
    action="" 은 미해석 callback_data — ack 후 무시.
    """
    channel_id = event.channel_id
    adapter.ack(event.callback_id)  # 로딩 스피너 종료
    action = event.action
    if not action:
        return  # 알 수 없는 callback_data 는 무시(ack 만)
    message_id = event.message_id

    if action == "clean:x":
        # 청소 확인의 «✖ 취소» — 답장 없이 확인 메시지 자체를 지운다.
        log.info("chat=%s callback 청소 취소", channel_id)
        if isinstance(message_id, int):
            adapter.delete_message(channel_id, message_id)
    elif action in ("clean:ok", "clean:link", "clean:all"):
        # 청소 확인 탭 → 무음(완료 메시지 없음, 개발자 요청). purge 가 확인 메시지까지 지워 채널이
        # 깨끗해지고 끝 — send/edit 안 함(edit 은 사라진 메시지라 실패).
        log.info("chat=%s callback %s", channel_id, action)
        if event.channel_role == SNS_ROLE:
            # 🔗 링크청소(clean:link — 이미 떠 있던 옛 확인의 clean:ok 도 같다) = 수집 → 수집기가 본
            # 범위에서 저장 안 된 링크 메시지는 남기고 정리. 🧹 전체청소(clean:all) = 수집 → 전부.
            # 지우기 전에 링크 수집(유실 방지) — 히스토리+삭제가 길어 데몬 스레드로.
            # 연타는 무시 — 두 번째 청소가 첫 청소가 보낸 저장 카드를 지우지 않게.
            if _sns_clean_lock.acquire(blocking=False):
                full = action == "clean:all"
                _sns_spawn("sns-clean", _clean_sns_locked, adapter, channel_id, full)
            else:
                log.info("chat=%s SNS 청소 진행 중 — 연타 무시", channel_id)
        else:
            # 다른 채널도 백그라운드 스레드 + «바쁨» — 14일 넘은 메시지 개별 삭제는 수 분이
            # 걸리는데, 바쁨 없이 돌면 자동 재시작이 진행 중인 purge 를 끊는다(2026-10-10 실측).
            # 연타는 채널별 락.
            lock = _channel_clean_lock(channel_id)
            if lock.acquire(blocking=False):
                _sns_spawn("clean", _clear_channel_locked, adapter, channel_id, lock)
            else:
                log.info("chat=%s 청소 진행 중 — 연타 무시", channel_id)
    elif action == "sns_judge":
        _handle_sns_judge(adapter, event)


def _is_playlist_command(text: str) -> bool:
    """플레이리스트 채널 화이트리스트: ㅁ노래·정지·다음·청소·추가·삭제·재생·목록·스포티파이(순수).

    실제 처리 분기(music_action·'ㅁ청소'·is_music_add/del/play_one/list/spotify)와 같은 조건이어야 —
    게이트만 통과하고 아래 분기에 안 걸리면 HELP 폴백이 새어 채널에 안내가 뜬다(§ 무반응 계약).
    ⚠️ 이건 **라우팅**(그 채널에서 처리되는가)이지 인가가 아니다 — ㅁ삭제도 여기선 True 여야 개발자가
    그 채널에서 쓸 수 있다. 비인가 멤버 차단은 _playlist_bypass 가 따로 한다.
    """
    stripped = text.strip()
    return (
        music_action(stripped) is not None
        or stripped == "ㅁ청소"
        or is_music_add(stripped)
        or is_music_del(stripped)
        or is_music_play_one(stripped)
        or is_music_list(stripped)
        or is_music_spotify(stripped)
    )


def _playlist_bypass(event: Event) -> bool:
    """★ user 인가 우회 지점(보안 감사 대상) — 플레이리스트 채널의 화이트리스트 음악 명령만.

    True 를 반환할 때만 handle_event 가 비인가 user_id 를 통과시킨다(서버 멤버 누구나 음악 제어,
    개발자 결정). 조건을 의도적으로 좁게 유지한다:
      · (channel_role == "playlist")  AND
      · text  → 화이트리스트 명령(_is_playlist_command) **에서 ㅁ삭제·ㅁ목록·ㅁ스포티파이는 뺀다**
        button → clean:ok/clean:x (ㅁ청소 확인·취소 — 봇이 이 채널서 내는 유일 버튼)

    🔴 **우회 판정 3조건** — 셋을 모두 지키는 명령만 넣는다(하나라도 어기면 뺀다):
      1. **상태변경 없음**(또는 되돌릴 수 있음) — 재생목록·레포·세션을 파괴하지 않는다
      2. **비용 유계** — 공격자가 반복해도 처리량이 그 명령의 상수배를 넘지 않는다
      3. **회신 유계** — 회신 건수·크기가 공격자 조종 하에 있지 않다

    ⚠️ 빠져 있는 셋:
      · **ㅁ삭제** — 1 위반(파괴적). 허용목록 유저(is_allowed)만 쓴다.
      · **ㅁ스포티파이** — 1·2 위반. 한 번에 **90곡**을 재생목록에 밀어넣고(되돌리려면 ㅁ삭제를
        90번 쳐야 한다), 명령 1건이 유튜브 검색 90회 + Data API 왕복 90회를 태워 단일 스레드
        코어를 수 분간 점유한다. 아무나 반복할 수 있으면 안 된다(2026-08-25 신설).
      · **ㅁ목록** — 2·3 위반(2026-08-18 운영자 결정). 비인가 멤버가 ㅁ추가로 목록을 불린 뒤
        ㅁ목록을 반복하면 **회신 크기와 내용이 둘 다 공격자 손 안에** 있고, 단일 스레드 코어가
        다중 페이지 API + 다중 메시지를 동기로 처리해 브리지 전체가 막힌다. 상한·캐시를 다는
        대신 우회에서 빼는 것이 최소 수정이다.
    셋 다 라우팅(_is_playlist_command)은 True 라 개발자는 그 채널에서 그대로 쓰고, 비인가 멤버에겐
    무반응이다. ㅁ재생은 ㅁ노래·ㅁ다음과 같은 급(재생 제어)이고 회신이 한 줄이라 3조건을 지킨다.
    그 외(다른 채널·비화이트리스트 텍스트·위험명령 ㅁ재시작)는
    False → 기존 is_allowed 인가 그대로. 위험명령은 플레이리스트 게이트가 이미 무시하므로 비인가
    user 에게 도달 불가(이중 방어). channel_role 은 어댑터가 channel_map 으로 채운 신뢰값.
    """
    if event.channel_role not in _MUSIC_ONLY_ROLES:
        return False
    if event.kind == "text":
        return _is_playlist_command(event.text) and not (
            is_music_del(event.text) or is_music_list(event.text) or is_music_spotify(event.text)
        )
    if event.kind == "button":
        return event.action in ("clean:ok", "clean:x")
    return False


def _format_add_result(result: tuple[str, str], channel: str = "") -> str:
    """youtube.add_video 결과(status, detail) → 회신 **한 줄**(2026-08-18 운영자 요청 형식).

    added/dup 의 detail 은 **유튜브가 준 제목**(제3자 입력)이라 display_title 로 다듬은 뒤
    escape_reply 로 감싼다. channel 은 가수 채우기용(Topic 채널일 때만 쓰인다 — display_title).
    fail 의 detail 은 youtube._reason 이 만든 내부 문구라 감싸지 않는다(비밀값·외부 입력 없음).
    """
    status, detail = result
    if status == "added":
        return f"✅ 추가({escape_reply(display_title(detail, channel))})"
    if status == "dup":
        return f"이미 있음({escape_reply(display_title(detail, channel))})"
    return f"추가 실패({detail})"


def _add_one(adapter: Adapter, video_id: str) -> tuple[str, str]:
    """영상 1건 추가 + (신규추가 & 재생 중이면) 재생 큐 실시간 편입. youtube.add_video 결과 그대로.

    중복(dup)은 이미 재생목록에 있어 큐에도 있으므로 편입 안 함. 재생 중 아니면 enqueue_video 가
    no-op(0) → 어차피 다음 ㅁ노래에 자연 포함된다.
    회신 문구가 필요한 호출부는 _add_one_line 을, 상태 집계만 필요한 호출부(ㅁ스포티파이)는
    이 함수를 직접 쓴다 — 추가 경로는 하나뿐이어야 한다.
    """
    result = youtube.add_video(video_id)
    if result[0] == "added":
        adapter.enqueue_video(video_id, result[1])
    return result


def _add_one_line(adapter: Adapter, video_id: str, channel: str = "") -> str:
    """영상 1건 추가(_add_one) + 회신 한 줄.

    🔴 편입 결과는 **회신에 싣지 않는다**(2026-08-18 운영자 지시 — 회신은 한 줄). 동작은 그대로다.
    """
    return _format_add_result(_add_one(adapter, video_id), channel)


def _handle_music_list(adapter: Adapter, channel_id: int) -> None:
    """'ㅁ목록' — 재생목록 전곡을 번호·제목으로 회신(읽기 전용).

    99곡이면 디스코드 2000자 한도를 넘으므로 **줄 경계로 나눠 여러 메시지**로 보낸다
    (어댑터의 chunk_text 는 2000자에서 무자비하게 잘라 제목이 두 동강 난다).
    🔴 제목은 **제3자가 올린 문자열**이라 escape_reply 를 통과시킨다(목록은 그 제목이 한 번에
    100줄 나가는 최대 노출면이다). `목록 실패: {reason}` 은 인가 유저만 보고 내부 문구다.
    """
    reason, items = youtube.list_titles()
    if reason:
        adapter.send(channel_id, f"목록 실패: {reason}")
        return
    if not items:
        adapter.send(channel_id, "재생목록이 비어 있습니다")
        return
    lines = [f"🎵 재생목록 {len(items)}곡"]
    lines += [f"{i}. {escape_reply(title)}" for i, (_vid, title) in enumerate(items, 1)]
    for part in pack_lines(lines, MUSIC_LIST_MSG_LIMIT):
        adapter.send(channel_id, part)


def _handle_music_add(adapter: Adapter, channel_id: int, text: str) -> None:
    """'ㅁ추가' 처리. 링크(들)면 videoId 추출해 각각 추가, 아니면 검색어로 후보 5건 중 1건 추가.

    링크+캡션 = 링크만 처리(캡션 무시). 다중 링크 = 각각 처리(중복은 add_video 가 개별 스킵).
    재생목록 전용 링크(videoId 없음)는 개별 실패. 네트워크는 위임 — list/insert 는 youtube 모듈
    (stdlib urllib), 검색은 adapter.search_candidates(yt-dlp) **1회**. 여기선 파싱·선택(pick_index)·
    회신만 한다. 검색어 경로는 '#N' 으로 순번을 직접 고를 수 있다.
    🔴 **성공 회신은 한 줄**(`✅ 추가(<제목>)`)이다 — 후보 목록·큐 편입 문구는 2026-08-18 운영자
    지시로 뺐다. `#N` 은 후보가 안 보여도 그대로 쓸 수 있게 **유지**한다(되살리지 말 것).
    """
    arg = _cmd_arg(text)
    if not arg:
        adapter.send(channel_id, "추가 실패(유튜브 링크나 검색어를 주세요)")
        return
    url_tokens = [t for t in arg.split() if is_youtube_url(t)]
    if url_tokens:  # 링크 우선(캡션 무시) — 각 링크를 개별 처리
        lines = []
        for t in url_tokens:
            vid = extract_video_id(t)
            if vid is None:  # 재생목록 전용 링크 등 videoId 없음
                lines.append("추가 실패(개별 영상 링크를 주세요)")
            else:
                lines.append(_add_one_line(adapter, vid))  # 링크는 채널을 모른다 → 가수 채우기 없음
        adapter.send(channel_id, "\n".join(lines))
        return
    query, index = parse_add_index(arg)
    candidates = adapter.search_candidates(query)  # [(videoId, 제목, 채널)] 최대 5건
    picked = pick_index(candidates, index, query)
    if picked is None:
        if candidates:  # 후보는 있는데 '#N' 이 범위 밖 — 조용히 다른 곡으로 바꿔치지 않는다
            n = len(candidates)
            # 괄호 안에 괄호를 겹치지 않는다 — `실패(#5 번 … 없습니다(1~1))` 는 읽기 어려웠다.
            adapter.send(
                channel_id, f"추가 실패(#{index} 번 후보가 없습니다 — 1~{n} 중에서 고르세요)"
            )
        else:
            adapter.send(channel_id, f"추가 실패({escape_reply(query)} 검색 결과가 없습니다)")
        return
    vid, _title, ch = candidates[picked]
    adapter.send(channel_id, _add_one_line(adapter, vid, ch))


def _handle_music_del(adapter: Adapter, channel_id: int, text: str) -> None:
    """'ㅁ삭제 <제목>' 처리 — 유튜브 재생목록에서 제거(+재생 중이면 큐에서도 뺀다).

    판정·삭제는 youtube.remove_video 소관(stdlib Data API — 추가와 대칭). 여러 곡이 걸리면
    지우지 않고 후보를 돌려주므로(오삭제 방지) 여기선 그 4갈래를 회신 문구로 옮기기만 한다.
    """
    arg = _cmd_arg(text)
    if not arg:
        adapter.send(channel_id, "삭제 실패: 지울 노래 제목을 주세요")
        return
    status, detail, video_id = youtube.remove_video(arg)
    if status == "removed":
        line = f"🗑️ 삭제됨: {escape_reply(detail)}"
        dropped = adapter.dequeue_video(video_id)  # 재생 중이 아니면 0(no-op)
        if dropped > 0:
            line += f"\n(재생 큐에서 {dropped}곡 제거)"
    elif status == "none":
        # 🔴 힌트를 붙인다 — 화면에 뜬 제목을 그대로 쳐도 안 걸릴 수 있다. display_title 이 **원본에
        # 없는 가수**를 앞에 붙여 보여주기 때문이다(`사랑하니까` → `더 크로스 - 사랑하니까`).
        # 매칭을 느슨하게 푸는 쪽으로 해결하지 마라 — 파괴적 명령이라 오삭제가 더 비싸다.
        line = (
            f"삭제 실패: {escape_reply(arg)} 를 재생목록에서 못 찾았습니다"
            " (가수 부분을 빼고 곡명만 쳐보세요)"
        )
    elif status == "many":
        line = f"여러 곡이 걸립니다 — 더 정확히 적어주세요:\n{escape_reply(detail, 250)}"
    else:
        line = f"삭제 실패: {detail}"
    adapter.send(channel_id, line)


# ── 'ㅁ스포티파이' — kworb 미러의 스포티파이 주간차트 → 재생목록 일괄 추가 ────────────
# 스포티파이 공식 API 는 쓰지 않는다(차트 엔드포인트가 막혀 있고 OAuth 가 필요하다). kworb 가
# 미러링한 HTML 을 allowlist GET(fetch_digest_text) 으로 받아 「가수 곡명」 검색어만 뽑는다.
SPOTIFY_CHARTS = (
    ("글로벌", "/spotify/country/global_weekly.html"),
    ("일본", "/spotify/country/jp_weekly.html"),
    ("한국", "/spotify/country/kr_weekly.html"),
)
SPOTIFY_TOP_N = 30  # 차트당 상위 N곡(3개 차트 → 최대 90곡)
_KWORB_QUERY_MAX = 120  # 검색어 상한(외부 문자열 — 길이를 공격자에게 맡기지 않는다)
# 곡 셀: `<td class="text mp"><div><a …/artist/…>가수</a> - <a …/track/…>곡명</a>…</div></td>`.
# 가수 링크를 따로 잡지 않는 이유 — **가수가 링크가 아닌 행이 있다**(`Unknown Artist - <a track>`,
# 2026-08-25 jp 차트 200행 중 1행). 트랙 링크 앞을 통째로 잡아 태그만 걷어내면 두 모양을 다 먹는다.
# 가수 캡처가 `(.*?)` 가 아닌 이유 **둘**:
# ① 길이 상한 `{0,300}` — 무한 `.*?` 는 `<td class="text mp"><div>` 접두를 만날 때마다 줄 끝까지
#    재확장해 실패 시 O(N²)다(300KB=_DIGEST_MAXBYTES 상한에서 10.4초 실측 · 차트 3개면 코어가
#    ~31초 멈춘다). 300 = 실측 여유값: 실물 행의 이 캡처는 가수 링크를 여럿 이어도 100자를 안
#    넘는다(픽스처 32행 최대 76자). 넘는 행은 그 행만 빠진다.
# ② 셀 경계 `(?!</div>)` — 「개행이 막아준다」는 **kworb 가 셀마다 줄바꿈을 넣는 지금 서식에만**
#    기대는 근거였다. 한 줄에 두 셀이 붙으면(minify) 트랙 링크가 없는 앞 셀이 뒷 셀을 삼켜
#    `공지</div></td><td…><div>B - <a track>U` 가 `"공지B U"` 로 뽑힌다 — 예외도 로그도 없이
#    엉뚱한 곡 30개가 재생목록에 들어간다. `</div>` 를 못 넘게 해 캡처를 셀 안에 가둔다.
_KWORB_ROW_RE = re.compile(
    r'<td class="text mp"><div>((?:(?!</div>).){0,300}?)<a href="[^"]*/track/[^"]*">([^<]*)</a>'
)
_KWORB_TAG_RE = re.compile(r"<[^>]*>")


def parse_kworb_tracks(page: str, limit: int) -> list[str]:
    """kworb 차트 HTML → 상위 limit 곡의 '가수 곡명' 유튜브 검색어(순수). 실패·빈 페이지는 [].

    🔴 여기서 나오는 문자열은 **외부에서 온 데이터이지 지시가 아니다**. 그래서 이 함수가
    한 줄 필드로 정규화(strip_control_line — 제어문자 제거 + 공백 접기)하고 길이를 자른다.
    회신에 실릴 일이 생기면 호출부가 escape_reply 를 **반드시** 통과시켜야 한다
    (ㅁ스포티파이 회신은 곡 제목을 아예 싣지 않는다 — 집계 숫자만 나간다).
    """
    out: list[str] = []
    for raw_artist, raw_title in _KWORB_ROW_RE.findall(page):
        artist = _KWORB_TAG_RE.sub("", raw_artist).strip().rstrip("-").strip()
        query = strip_control_line(html.unescape(f"{artist} {raw_title}"))[:_KWORB_QUERY_MAX]
        if query:
            out.append(query)
            if len(out) >= limit:
                break
    return out


def _handle_music_spotify(adapter: Adapter, channel_id: int, month: str) -> bool:
    """'ㅁ스포티파이' — 주간차트 3개의 상위 30곡을 재생목록에 담고 집계만 회신한다.

    곡 하나를 넣는 경로는 ㅁ추가와 **같다**(adapter.search_video → _add_one → youtube.add_video).
    중복 판정도 add_video 가 insert 전에 하므로 여기서 새로 만들지 않는다.
    90회 검색이 순차로 돌아 수 분 걸린다 → 먼저 "시작" 을 보내고 끝나면 요약을 보낸다.
    - **수동 경로**(ㅁ스포티파이)는 이벤트 워커가 직렬 처리하므로 그동안 다른 명령이 대기한다
      (실행 명령과 같은 성질).
    - **자동 경로**(run_spotify_monthly)는 _start_digest 가 띄운 **데몬 스레드**에서 돌아 워커를
      막지 않는다 — 그동안 ㅁ추가·ㅁ삭제가 동시에 들어온다. 그래서 youtube 모듈이 `_LOCK` 으로
      호출 1건씩 직렬화한다(2026-08-25 — 그 전까지 youtube.py 는 "단일 워커" 전제였다).
    한 곡의 실패(검색 무결과·네트워크·API 오류)에 여기서 try/except 를 걸지 않는 이유 —
    **호출부가 계약상 예외를 던지지 않는다**: search_video·youtube.add_video·fetch_digest_text 가
    각자 삼켜 None·("fail",…)·"" 로 돌려주므로, 여기서는 그 반환을 실패 카운트로 세기만 한다.

    month = 스탬프에 찍을 `YYYY-MM` — **호출부가 확정해 넘긴다**(수동 = 명령을 받은 시각의 달,
    자동 = 러너가 판정에 쓴 달). 🔴 기본값을 두지 않는 것이 계약이다 — 왜는 _mark_spotify_month.

    반환 = **담기를 시도했는지**(False = 시작 안내조차 못 보내 아무 일도 안 함).
    월 1회 자동 실행(run_spotify_monthly)이 이 값으로 "그 달 몫을 썼는지"를 판단한다.
    """
    # ⚠️ send 를 try/except 로 감싸지 마라 — 어댑터는 계약상 예외 없이 실패를 None 으로 돌린다
    # (§3.3). 시작 안내가 실패했다 = 이 채널에 말을 못 붙인다 = 결과를 전할 곳이 없다는 뜻이라,
    # 90회 왕복을 시작하지 않고 그대로 접는다(자동 실행은 스탬프 없이 다음 틱에 다시 잡는다).
    # 2줄인 이유 — 한 줄이면 «도는 중인지 멈춘 건지» 알 수 없다(2026-08-25 운영자 지적·문안 지정).
    # 예상 시간은 그날 실측 6분 59초(90곡). **재생목록이 커질수록 add_video 의 목록 재조회가
    # 길어져 늘어난다** — 크게 어긋나면 이 숫자를 조정한다.
    opening = "🎧 스포티파이 월간차트 추가\n차트를 가져오고 있습니다(7분 예상)"
    if adapter.send(channel_id, opening) is None:
        log.warning("chat=%s spotify 시작 안내 실패 — 담지 않고 중단", channel_id)
        return False
    added = dup = fail = 0
    seen: set[str] = set()  # 차트끼리 겹치는 곡 — 두 번째부터는 왕복 없이 '이미 있음'
    missing: list[str] = []
    for name, path in SPOTIFY_CHARTS:
        queries = parse_kworb_tracks(fetch_digest_text(path), SPOTIFY_TOP_N)
        if not queries:  # 조회 실패·구조 변경 — 그 차트만 건너뛰고 나머지는 계속한다
            missing.append(name)
            continue
        for query in queries:
            hit = adapter.search_video(query)
            if hit is None:
                fail += 1
                continue
            if hit[0] in seen:
                dup += 1
                continue
            seen.add(hit[0])
            status = _add_one(adapter, hit[0])[0]
            if status == "added":
                added += 1
            elif status == "dup":
                dup += 1
            else:
                fail += 1
    log.info(
        "chat=%s spotify 추가=%d 중복=%d 실패=%d 차트실패=%d",
        channel_id,
        added,
        dup,
        fail,
        len(missing),
    )
    # 🔴 **수동 'ㅁ스포티파이' 도 스탬프를 갱신한다**(그래서 러너가 아니라 여기서 찍는다).
    # 스탬프의 목적이 "한 달에 한 번만 90회 왕복" 인데, 관리자가 손으로 담은 직후 다음 세션의
    # 자동 실행이 같은 주간차트를 한 번 더 훑으면 그 목적이 그대로 깨진다(추가는 거의 0곡,
    # 왕복만 90회). 반대 방향은 막지 않는다 — 수동은 스탬프를 **읽지 않아** 언제든 다시 돈다.
    # ⚠️ **전 차트 실패(missing 3개)면 찍지 않는다** — 재시도가 비싸다는 종전 근거가 사실과
    # 달랐다: 차트를 하나도 못 읽으면 queries 가 비어 90회 왕복을 **아예 안 하고** HTTP 3회로
    # 끝난다(거의 공짜다). 일시적 네트워크 오류 한 번에 그 달 90곡을 통째로 날릴 이유가 없다.
    # 부분 실패(1~2개)는 지금처럼 찍는다 — 이미 담은 게 있어 재실행하면 성공한 차트를 또 훑는다.
    # (반환은 True 그대로 — 그 세션엔 재시도하지 않고 다음 세션에 한 번 다시 잡는다.)
    if len(missing) < len(SPOTIFY_CHARTS):
        _mark_spotify_month(month)
    # 운영자 지정본(2026-08-25) — 제목 없이 `✅처리완료`(공백 없음) + 항목당 한 줄.
    # 시작 안내에 제목이 이미 있어 문맥이 이어진다.
    line = f"✅처리완료\n추가 {added}곡\n중복 {dup}곡\n실패 {fail}곡"
    # 차트 실패는 **로그로만** 남긴다(2026-08-25 운영자 지시 — 회신은 지정본 4줄 고정).
    # 스탬프 판정은 위 `missing` 이 그대로 쓴다(전 차트 실패면 안 찍는다).
    adapter.send(channel_id, line)
    return True


# ── 월 1회 자동 실행 — 스탬프가 주기를 정한다 ──────────────────────────────────
# 이 PC 는 상시 가동이 아니다(관리자가 켜 둔 동안만 돈다). "매월 1일 00:00" 으로 잡으면
# 그날 PC 가 꺼져 있던 달은 **통째로 건너뛴다** → notify.json 에서는 매일 09:00~23:59 창으로
# 통과시키고, 실제 주기는 이 스탬프 파일이 정한다.
SPOTIFY_MONTH_F = LOG_DIR / "spotify_month.txt"  # 마지막으로 담은 달 `YYYY-MM` 한 줄


def _mark_spotify_month(month: str) -> None:
    """`YYYY-MM` 스탬프 기록 — 실패해도 담은 것은 성공이라 삼킨다(기록 실패로 되돌리지 않는다).

    🔴 **찍는 달을 인자로 받는 이유 — 비교한 값을 그대로 찍어야 한다.** 여기서
    `datetime.now()` 를 다시 읽으면
    판정 시각(러너 시작)과 기록 시각(90회 왕복 뒤)이 갈라진다 — 8/31 23:5x 에 시작해 00:0x 에
    끝나면 `2026-09` 가 찍히고, 9월 첫 세션이 "이미 담았다"로 조용히 끝나 **9월치가 통째로
    사라진다**(2026-08-25 재현 확인).

    최악의 결과 = 다음 세션에 한 번 더 담는다(중복은 add_video 가 '이미 있음' 으로 거른다).
    """
    try:
        SPOTIFY_MONTH_F.parent.mkdir(parents=True, exist_ok=True)
        SPOTIFY_MONTH_F.write_text(month + "\n", encoding="utf-8")
    except OSError as e:
        log.warning("스포티파이 월 스탬프 기록 실패(%s)", type(e).__name__)


def run_spotify_monthly(adapter: Adapter, channel_id: int, today: str) -> bool:
    """월 1회 스포티파이 주간차트 담기. 반환 = 이 항목을 처리 완료로 볼지.

    달이 바뀐 뒤 PC 를 **처음 켠 날** 한 번 돈다(1일에 꺼져 있어도 놓치지 않는다).
    곡 수집·추가는 전부 수동 'ㅁ스포티파이' 와 **같은 핸들러**가 한다 — 여기는 껍데기다.

    이미 이번 달에 담았으면 **True**(조용히 끝). "할 일이 없다"는 실패가 아니다 — False 로
    돌리면 _revert_digest_fired 가 fired 를 풀어 25초마다 같은 판정을 DIGEST_MAX_ATTEMPTS 회
    반복하고 WARNING 을 남긴다.
    """
    try:
        if SPOTIFY_MONTH_F.read_text(encoding="utf-8").strip() == today[:7]:
            return True
    except FileNotFoundError:
        pass  # 아직 한 번도 안 담았다 — 정상 경로라 조용히 지나간다
    except (OSError, ValueError) as e:
        # 스탬프를 못 읽으면 주기 제한이 통째로 꺼져 **매 세션 90회 왕복**이 돈다. 흔적이 0 이면
        # 아무도 모르므로 경고는 남긴다(읽기 실패로 담기를 포기하지는 않는다 — 그게 이 기능이다).
        # 🔴 ValueError 도 잡는 이유 — `UnicodeDecodeError` 는 **OSError 가 아니라 ValueError** 다.
        # 파일에 비UTF-8 바이트가 한 번 들어가면 여기서 예외가 새어 _run_digest 가 삼키고(3회 재시도
        # 뒤 그날 포기) **덮어쓰는 경로에 영영 도달하지 못해** 다음 달도 그 다음 달도 고장난다.
        # 디코드 실패 = 파손된 스탬프 = 다시 담아야 정상이므로 「못 읽었다」와 같게 취급한다.
        # ⚠️ 스탬프 파일을 새로 들일 때마다 같은 실수가 난다 — 읽기는 `(OSError, ValueError)`
        #    둘 다 잡아라.
        log.warning(
            "스포티파이 월 스탬프를 못 읽었다(%s) — 이번 달에 또 담을 수 있다", type(e).__name__
        )
    # 판정에 쓴 달(today[:7])을 그대로 넘긴다 — 왜는 _mark_spotify_month 주석.
    return _handle_music_spotify(adapter, channel_id, today[:7])


# ══════════════════════════════════════════════════════════════════════════
# 📥 SNS정보 → 옵시디언 수집함 (정오 러너 · 🔍 판정하기 버튼 · 완료 신호)
# ══════════════════════════════════════════════════════════════════════════
# 정본 = docs/기능/SNS정보_수집/01_계획.md. 링크 추출·노트·문구는 sns_inbox(순수), 여기는 배선만.
SNS_ROLE = "SNS정보"  # 채널 tag(discord_adapter._SPECIAL) — notify.json 의 channel 도 이 값
SNS_STATE_FILE = LOG_DIR / "sns_state.json"  # last_message_id·last_run_date·pending_cards
SNS_DONE_FILE = LOG_DIR / "sns_judge_done.json"  # VS Code 판정 세션이 남기는 완료 신호
SNS_INBOX_DIR = REPO_ROOT / "Hachiware" / "_Obsidian" / "수집함"  # 레포 상대 고정(계획 §1)
# 판정 입구(muhwa-dev `python -m core.judge_inbox`) — 고정 argv, 사용자 입력은 싣지 않는다.
JUDGE_PROJECT_DIR = REPO_ROOT / "Hachiware" / "_Project" / "muhwa-dev"
JUDGE_TIMEOUT_SEC = 60
# 한 번에 읽는 히스토리 상한(어댑터가 100건씩 페이지로 받는다) — 넘치면 처리한 데까지 전진하고
# 나머지는 다음 실행. 통째 실패로 같은 자리에서 영원히 멈추지 않게 한다.
SNS_HISTORY_MAX = 1000
SNS_PENDING_STALE_SEC = 24 * 3600  # 이보다 오래 «판정 중» 인 카드는 정오 러너가 되돌린다
_URL_SCHEME_RE = re.compile(r"(https?)://", re.IGNORECASE)  # 판정완료 카드 칸 값의 생 URL
# 완료 신호 크기 상한 — `항목`(칸당 200자, 여러 건)이 들어가 32KB. 넘으면 읽지 않고 스키마 오류.
_SNS_DONE_MAX_BYTES = 32 * 1024
_SNS_FIELD_MAXLEN = 200  # 판정완료 카드 칸 값 상한(초과분은 잘라 `…`)
# 판정완료 카드 한 통의 길이 예산. 카드는 상태 헤더가 아니라 **일반 메시지**(디스코드 2000자)로
# 나간다 — 마스킹(***)이 길이를 늘릴 수 있어 MUSIC_LIST_MSG_LIMIT 과 같은 여유를 둔다.
_SNS_JUDGE_CARD_LIMIT = 1800
# 판정완료 카드 항목 스키마(판정_절차.md §4) — 판정별 필수 칸. 값은 전부 문자열.
_SNS_ITEM_FIELDS = {
    "사용": ("제목", "무엇", "추가", "쓰는법"),
    "폐기": ("제목", "사유"),
}
_SNS_DONE_STAMP_MAXLEN = 64  # `시각` 길이 상한(ISO 시각이면 30자 안팎)
SNS_JUDGE_BUTTON = Button(sns_inbox.JUDGE_LABEL, "sns_judge", style="primary")
# 히스토리 허용목록 — main 이 .env 의 허용목록으로 채운다(비어 있으면 아무것도 저장하지 않는다).
# ponytail: 모듈 상수 대입(youtube.PLAYLIST_ID 방식) — 러너 시그니처가 DIGEST_RUNNERS 공용이라.
sns_allowed: frozenset[int] = frozenset()
# 상태 파일은 러너 스레드·워커(버튼)·타이머(완료 신호) 셋이 쓴다 → 읽기-수정-쓰기는 이 락 아래서.
_sns_lock = threading.RLock()
_sns_clean_lock = threading.Lock()  # #SNS정보 청소 1건씩(연타 무시) — 클릭 때 잡고 스레드가 푼다
# 스키마 오류 경고를 파일 (mtime, 크기) 당 1번만(25초 틱 도배 방지)
_sns_done_warned: tuple[float, int] | None = None
# #SNS정보 채널 미매핑 경고도 같은 방식으로 파일 (mtime, 크기) 당 1번만
_sns_channel_warned: tuple[float, int] | None = None

# 디스코드 snowflake = (ms - 2015-01-01) << 22. 공유 시각과 «지금» 기준 id 를 여기서 구한다.
# ponytail: 플랫폼 지식이 코어에 1줄 샌다 — 어댑터가 바뀌면 Event 에 created_at 을 싣는다.
_DISCORD_EPOCH_MS = 1_420_070_400_000


def snowflake_time(message_id: int) -> datetime:
    return datetime.fromtimestamp(((message_id >> 22) + _DISCORD_EPOCH_MS) / 1000, _KST)


def snowflake_now() -> int:
    return (int(time.time() * 1000) - _DISCORD_EPOCH_MS) << 22


def _load_sns_state() -> dict[str, Any]:
    try:
        raw = json.loads(SNS_STATE_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:  # ValueError = JSON 손상·비UTF-8
        log.warning("sns_state 를 못 읽었다(%s) — 첫 실행으로 취급", type(e).__name__)
        return {}
    return raw if isinstance(raw, dict) else {}


def _sns_update(**changes: Any) -> None:
    """상태 파일의 필드만 바꿔 원자 저장. 값이 None 이면 그 키를 지운다.

    🔴 파일이 **없을 때만** 빈 상태에서 시작한다. 읽기 오류·JSON 손상이면 OSError 로 올리고
    **덮어쓰지 않는다** — 빈 dict 로 덮으면 pending_cards·baseline_id 같은 남은 필드가 통째로
    사라진다. 호출측은 기존 실패 경로(OSError)로 처리한다.
    """
    with _sns_lock:
        try:
            raw = json.loads(SNS_STATE_FILE.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raw = {}
        except ValueError as e:  # JSON 손상·비UTF-8 — 덮어쓰지 않고 실패로 올린다
            raise OSError(f"sns_state 손상({type(e).__name__}) — 덮어쓰지 않음") from e
        if not isinstance(raw, dict):
            raise OSError("sns_state 형식 오류 — 덮어쓰지 않음")
        state = raw
        for k, v in changes.items():
            if v is None:
                state.pop(k, None)
            else:
                state[k] = v
        SNS_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        sns_inbox.write_atomic(SNS_STATE_FILE, json.dumps(state, ensure_ascii=False))


def sns_init_state() -> bool:
    """상태가 없으면 last_message_id 를 «지금» 으로 심는다(과거 백필 없음). 심었으면 True.

    main 이 기동 때 한 번 부른다 — 그래야 첫 정오 전에 공유한 것도 첫 실행에 잡힌다.
    """
    with _sns_lock:
        if isinstance(_load_sns_state().get("last_message_id"), int):
            return False
        now = snowflake_now()
        # baseline_id = 수집기가 «보기 시작한» 지점. 청소는 (baseline_id, last_message_id] 만 지운다
        # — 그 앞은 수집기가 본 적 없는 메시지라(상태 파일 유실 뒤 재기동 등) 지우면 유실이다.
        try:
            _sns_update(last_message_id=now, baseline_id=now)
        except OSError:
            # 손상된 상태 파일을 덮어쓰지 않는다(main 기동 경로라 죽지 않는다) — 사람이 고칠 때까지
            # 수집·청소는 시작점이 없는 것으로 남는다(청소는 지우지 않는다).
            log.exception("SNS 시작점 기록 실패 — 상태 파일 확인 필요")
            return False
    log.info("SNS 수집 시작점 기록(과거 백필 없음)")
    return True


def _pending_cards(state: dict[str, Any]) -> dict[str, str]:
    """«판정 중» 카드 {카드 id(str): 본문}. 옛 단일값(pending_card_id/_text)도 읽는다(호환)."""
    raw = state.get("pending_cards")
    cards: dict[str, str] = {}
    if isinstance(raw, dict):
        cards = {
            k: v
            for k, v in raw.items()
            if isinstance(k, str) and k.isascii() and k.isdigit() and isinstance(v, str)
        }
    old = state.get("pending_card_id")
    if isinstance(old, int) and not isinstance(old, bool):
        text = state.get("pending_card_text")
        cards.setdefault(str(old), text if isinstance(text, str) else sns_inbox.JUDGING_LINE)
    return cards


def _pending_since(state: dict[str, Any], cards: dict[str, str]) -> dict[str, float]:
    """카드별 «판정 중» 이 된 시각(epoch). 기록이 없으면(옛 상태) 카드가 올라온 시각(snowflake)."""
    raw = state.get("pending_since")
    since = raw if isinstance(raw, dict) else {}
    out: dict[str, float] = {}
    for k in cards:
        v = since.get(k)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            out[k] = float(v)
        else:
            out[k] = snowflake_time(int(k)).timestamp()
    return out


def _set_pending_cards(cards: dict[str, str], added_at: dict[str, float] | None = None) -> None:
    """목록 저장 — 옛 단일값 키는 함께 걷는다(새 형식으로 넘어간다). 빈 목록은 키 삭제.

    pending_since 는 남은 카드만 유지하고 added_at(새로 «판정 중» 이 된 카드)을 더한다.
    """
    with _sns_lock:
        since = {
            k: v
            for k, v in _pending_since(_load_sns_state(), cards).items()
            if k not in (added_at or {})
        }
        since.update({k: v for k, v in (added_at or {}).items() if k in cards})
        _sns_update(
            pending_cards=cards or None,
            pending_since=since or None,
            pending_card_id=None,
            pending_card_text=None,
        )


def _restore_stale_cards(adapter: Adapter, channel_id: int, now: float | None = None) -> None:
    """24시간 넘게 «판정 중» 인 카드를 원래 본문 + 🔍 판정하기 버튼으로 되돌리고 목록에서 뺀다.

    판정 세션을 시작하지 않고 닫으면 완료 신호가 영영 안 와 카드가 목록에 남는다 — 그대로 두면
    다음 완료 신호 1건이 옛 카드까지 🎉 로 바꾸고, 사람은 그 카드를 다시 누를 수도 없다.
    정오 러너가 부른다. 새 문구 없음 — 원래 카드 본문을 복원한다.
    """
    now = time.time() if now is None else now
    with _sns_lock:
        state = _load_sns_state()
        cards = _pending_cards(state)
        since = _pending_since(state, cards)
        stale = [k for k in cards if now - since[k] > SNS_PENDING_STALE_SEC]
        if not stale:
            return
        suffix = "\n" + sns_inbox.JUDGING_LINE
        for k in stale:
            judging = cards.pop(k)
            card = "" if judging == sns_inbox.JUDGING_LINE else judging.removesuffix(suffix)
            if card:  # 본문을 모르면(옛 상태) 목록에서만 뺀다 — 빈 카드로 덮지 않는다
                adapter.edit(channel_id, int(k), card, [SNS_JUDGE_BUTTON])
        _set_pending_cards(cards)
    log.info("SNS 오래된 «판정 중» 카드 %d장 되돌림", len(stale))


def _save_sns_notes(events: list[Event]) -> tuple[int, int, int, int, list[date]]:
    """허용 사용자 메시지의 인스타·X 링크를 미판정 노트로 쓴다 → (인스타, X, 중복, 링크X, 공유일들).

    중복은 (플랫폼, 게시물 id) 로 본다 — 수집함 전 노트의 `출처` + 이번 묶음. 이름이 겹치는데
    키가 다르면(대소문자만 다른 shortcode 가 NTFS 에서 한 파일이 되는 경우) `_2` 를 붙여 저장한다.
    쓰기 예외(OSError)는 그대로 올린다 — 호출측이 sns_fail 을 내고 last_message_id 를 멈춘다.
    """
    inbox = SNS_INBOX_DIR / "미판정"
    seen = sns_inbox.known_sources(SNS_INBOX_DIR)
    counts = {"insta": 0, "x": 0}
    dup = nolink = 0
    dates: list[date] = []
    for ev in events:
        if not isinstance(ev.message_id, int) or not is_allowed(ev.user_id, sns_allowed):
            continue  # 봇 자신의 카드·비허용 작성자 — 이벤트 경로와 같은 기준으로 버린다
        if ev.text.lstrip().startswith("ㅁ"):
            continue  # `ㅁ청소` 같은 봇 명령은 공유가 아니다 — «링크X» 로 세지 않는다
        links = sns_inbox.extract_links(ev.text)
        if not links:
            nolink += 1
            continue
        shared = snowflake_time(ev.message_id)
        for link in links:
            if link.key in seen:
                dup += 1
                continue
            path = sns_inbox.free_path(inbox, sns_inbox.note_name(link, shared))
            sns_inbox.write_atomic(path, sns_inbox.note_body(link, shared, ev.message_id))
            seen.add(link.key)  # 같은 묶음 안 중복도 한 번만
            counts[link.platform] += 1
            dates.append(shared.date())
    return counts["insta"], counts["x"], dup, nolink, dates


_SnsCounts = tuple[int, int, int, int, list[date]]  # (인스타, X, 중복, 링크X, 공유일들)


def _collect_sns(adapter: Adapter, channel_id: int, run_date: str | None) -> tuple[str, _SnsCounts]:
    """last_message_id 뒤 히스토리 → 미판정 노트 저장 → 시작점 전진. 정오 러너·청소 직전 공용.

    반환 상태: "ok"(저장 끝 — 0건 포함) · "more"(저장했지만 상한에 걸려 안 읽은 메시지가 남았을 수
    있다) · "fail"(폴더 없음·저장 실패, 안내를 이미 냈다) · "retry"(히스토리 읽기 실패, 안내 없음 —
    러너는 재시도, 청소는 공통 실패 안내) · "init"(첫 실행 — 시작점을 지금으로 심었을 뿐 아직 아무
    것도 읽지 않았다. 러너는 "ok" 와 같게 취급하고, 청소는 **절대 지우지 않는다** — 안 그러면 한
    건도 수집하지 않은 채 채널을 통째로 비우게 된다).
    run_date 가 있으면 last_run_date 도 기록한다(정오 러너만 — 청소는 «밀린 날» 판정에 손대지 않음).
    실패면 시작점이 그대로라 다음 실행이 같은 메시지를 다시 읽는다. 한 번에 SNS_HISTORY_MAX 건까지.
    """
    empty: _SnsCounts = (0, 0, 0, 0, [])
    if not (SNS_INBOX_DIR / "미판정").is_dir():
        log.warning("SNS 수집함 폴더 없음 — 저장 건너뜀(링크는 채널에 남는다)")
        post_system_notice(adapter, f"{notice_stamp()} {sns_inbox.NO_FOLDER_TEXT}")
        return "fail", empty
    # 🔴 «읽기 → 저장 → 전진» 전체를 한 락 아래서 — 정오 러너와 `ㅁ청소` 수집이 겹치면 둘 다
    # 같은 시작점에서 읽어 같은 게시물을 두 번(`_2`) 저장한다. RLock — 안의 _sns_update 는 재진입.
    with _sns_lock:
        state = _load_sns_state()
        last = state.get("last_message_id")
        if not isinstance(last, int):
            sns_init_state()  # 첫 실행 — 지금부터(백필 없음)
            return "init", empty
        events = adapter.history_after(channel_id, last, SNS_HISTORY_MAX)
        if events is None:
            log.warning("SNS 채널 히스토리 읽기 실패")
            return "retry", empty
        more = len(events) == SNS_HISTORY_MAX  # 받은 건수 == 상한 → 뒤가 더 있을 수 있다
        if more:
            log.warning(
                "SNS 히스토리 %d건 상한 — 나머지는 다음 실행이 이어 읽는다", SNS_HISTORY_MAX
            )
        ids = [ev.message_id for ev in events if isinstance(ev.message_id, int)]
        try:
            counts = _save_sns_notes(events)
            changes: dict[str, Any] = {"last_message_id": max(ids, default=last)}
            if not isinstance(state.get("baseline_id"), int):
                # 옛 상태(baseline 기록 전) — 이번 읽기의 시작점을 하한으로 삼는다. 그 앞은 이미
                # 수집했을 수도 있지만 «모른다» 쪽으로 기운다: 청소가 덜 지울 뿐 링크를 잃지 않는다.
                changes["baseline_id"] = last
            if run_date is not None:
                changes["last_run_date"] = run_date
            _sns_update(**changes)
        except OSError:
            log.exception("SNS 저장 실패 — last_message_id 유지(다음 실행 재시도)")
            post_system_notice(adapter, f"{notice_stamp()} {sns_inbox.FAIL_TEXT}")
            return "fail", empty
    log.info("SNS 수집 인스타=%d X=%d 중복=%d 링크X=%d", *counts[:4])
    return ("more" if more else "ok"), counts


def _send_sns_card(adapter: Adapter, channel_id: int, counts: _SnsCounts, catchup: bool) -> None:
    """저장 카드(sns_daily/sns_catchup) — 저장 0건이면 보내지 않는다(sns_none)."""
    text = sns_inbox.card_text(*counts, catchup)
    if text is not None and adapter.send(channel_id, text, [SNS_JUDGE_BUTTON]) is None:
        # 노트는 이미 저장됐다 — 되돌려 재시도하면 전부 «중복» 이 돼 카드가 영영 안 나간다.
        # 그래서 재시도 대신 사람에게 알린다(다이제스트 실패 공통 문구).
        log.warning("SNS 카드 게시 실패 — 노트는 저장됨(수집함 확인)")
        post_system_notice(adapter, digest_giveup_text(SNS_NOTIFY_ID))


def run_sns_inbox(adapter: Adapter, channel_id: int, today: str) -> bool:
    """정오 러너 — last_message_id 뒤의 공유를 수집함 노트로 만들고 카드를 낸다.

    반환 False = 히스토리 읽기 실패(다음 틱 재시도, 상한은 _revert_digest_fired). 폴더 없음·저장
    실패는 #봇상태 로 알리고 **True**(그날은 끝 — 25초마다 같은 알림을 내지 않는다).
    """
    _restore_stale_cards(adapter, channel_id)
    with _sns_lock:
        last_run = _load_sns_state().get("last_run_date")
    status, counts = _collect_sns(adapter, channel_id, today)
    if status not in ("ok", "more", "init"):  # 상한에 걸린 나머지는 다음 실행이 이어 읽는다
        return status == "fail"
    # 밀린 날 = 지난 실행일이 어제보다 이르다. 첫 실행(값 없음)·값 손상은 평소 카드.
    catchup = False
    if isinstance(last_run, str):
        with contextlib.suppress(ValueError):
            catchup = (date.fromisoformat(today) - date.fromisoformat(last_run)).days > 1
    _send_sns_card(adapter, channel_id, counts, catchup)
    return True


# 일반 채널 청소 연타 락 — 채널별(서로 다른 채널 청소는 동시에 돌아도 된다). #SNS정보 는
# _sns_clean_lock(수집 상태를 공유하는 채널이 하나뿐이라 전역 1개로 충분).
_clean_locks: dict[int, threading.Lock] = {}
_clean_locks_guard = threading.Lock()


def _channel_clean_lock(channel_id: int) -> threading.Lock:
    with _clean_locks_guard:
        return _clean_locks.setdefault(channel_id, threading.Lock())


def _clear_channel_locked(adapter: Adapter, channel_id: int, lock: threading.Lock) -> None:
    """일반 채널 청소 스레드 본체 — 클릭 때 잡은 채널 락을 끝나면 반드시 푼다."""
    try:
        adapter.clear_channel(channel_id)
    finally:
        lock.release()


def _clean_sns_locked(adapter: Adapter, channel_id: int, full: bool = False) -> None:
    """청소 스레드 본체 — 클릭 시점에 잡은 _sns_clean_lock 을 끝나면 반드시 푼다."""
    try:
        _clean_sns_channel(adapter, channel_id, full)
    finally:
        _sns_clean_lock.release()


def _clean_sns_channel(adapter: Adapter, channel_id: int, full: bool = False) -> None:
    """#SNS정보 청소 — 지우기 **전에** 정오 러너와 같은 수집을 한 번 돌려 링크를 잃지 않는다.

    full=False(🔗 링크청소) = 수집기가 본 범위에서 저장 안 된 링크가 든 메시지는 남긴다.
    full=True(🧹 전체청소) = 수집이 성공했을 때만 채널 **전체**(범위·keep 없음) — 저장 대상이 아닌
    링크도 지워진다(개발자 선택). 수집 실패·상한·시작점 없음이면 둘 다 지우지 않는다.

    순서 = 수집 → 청소 → 저장 1건 이상이면 저장 카드(청소가 카드까지 지우지 않게 청소 뒤에).
    수집이 실패하면 청소하지 않는다(링크를 남긴다) — 폴더 없음·저장 실패는 각자 안내를 이미 냈고,
    히스토리 읽기 실패는 공통 실패 안내를 낸다. last_run_date 는 건드리지 않는다.
    상한(SNS_HISTORY_MAX)에 걸려 안 읽은 메시지가 남았을 수 있으면 수집한 만큼만 저장(+카드)하고
    청소는 건너뛴다 — 공통 실패 안내 1번. 다시 누르면 이어 읽는다.
    """
    status, counts = _collect_sns(adapter, channel_id, None)
    if status == "init":
        # 시작점이 없던 첫 실행 — 아직 한 건도 읽지 않았다(시작점 없음 = 지우지 않는다).
        log.warning("SNS 청소 보류 — 시작점 없음(첫 실행), 채널 유지")
        post_system_notice(adapter, digest_giveup_text(SNS_NOTIFY_ID))
        return
    if status == "more":
        log.warning("SNS 청소 중단 — 히스토리 상한, 안 읽은 메시지가 남았을 수 있다")
        post_system_notice(adapter, digest_giveup_text(SNS_NOTIFY_ID))
        _send_sns_card(adapter, channel_id, counts, catchup=False)
        return
    if status != "ok":
        log.warning("SNS 청소 중단 — 수집 실패(%s), 링크 유지", status)
        if status == "retry":
            post_system_notice(adapter, digest_giveup_text(SNS_NOTIFY_ID))
        return
    if full:
        adapter.clear_channel(channel_id)  # 🧹 전체청소 — 수집을 마친 뒤 전부
        _send_sns_card(adapter, channel_id, counts, catchup=False)
        return
    # 🔴 **수집기가 본 범위만** 지운다 — (baseline_id, last_message_id]. 채널 전체를 지우면 상태
    # 파일이 사라져 시작점이 «지금» 으로 다시 심긴 뒤(브리지가 멈춘 사이 등) 그 앞 링크가 수집
    # 0건인 채 전멸한다. 수집 뒤에 올라온 메시지도 남는다(확인 메시지·카드는 범위 안이면 지워진다).
    with _sns_lock:
        state = _load_sns_state()
    base, upto = state.get("baseline_id"), state.get("last_message_id")
    if not isinstance(base, int) or not isinstance(upto, int) or upto <= base:
        log.info("SNS 청소 — 수집기가 본 범위가 비어 지울 것 없음")
    else:
        # 범위 안이어도 **수집함에 없는 링크가 든 메시지는 남긴다**(개발자 결정 A) —
        # 비허용 작성자의 인스타·X, 스토리·유튜브 같은 대상 밖 링크.
        # 저장된 링크만 있는 메시지·명령·잡담·카드는 지운다.
        try:
            saved = sns_inbox.known_sources(SNS_INBOX_DIR)
        except OSError:
            log.exception("SNS 청소 중단 — 수집함 출처를 못 읽음, 링크 유지")
            post_system_notice(adapter, digest_giveup_text(SNS_NOTIFY_ID))
            return
        adapter.clear_channel(
            channel_id,
            after_id=base,
            upto_id=upto,
            keep=lambda text, _author: sns_inbox.has_unsaved_link(text, saved),
        )
    _send_sns_card(adapter, channel_id, counts, catchup=False)


def _sns_spawn(name: str, fn: Callable[..., None], *args: Any) -> None:
    """SNS 의 느린 작업(판정 입구 60초·청소 전 수집+삭제)을 데몬 스레드로 — 워커를 막지 않게.

    다이제스트(_start_digest)와 같은 방식. 바쁨 표시(_working)로 감싸 자동 재시작이 그 사이에 끊지
    않게 하고, 스레드 안 예외는 역추적과 함께 로그로 남긴다(상위로 전파되지 않으니 유일한 증거다).
    """

    def run() -> None:
        try:
            fn(*args)
        except Exception:
            log.exception("SNS 작업 예외 (%s)", name)
        finally:
            _busy_add(-1)

    _start_busy_thread(run, name)


def launch_judge() -> bool:
    """AI비서 판정 입구를 고정 argv 서브프로세스로 부른다. 종료코드 0 = 성공.

    🔴 shell=False·고정 argv — 화면(디스코드)에서 온 문자열은 하나도 싣지 않는다.
    출력은 DEVNULL — 파이프를 열면 자식이 띄운 VS Code 가 핸들을 물려받아 timeout 까지 막힌다.
    """
    venv = JUDGE_PROJECT_DIR / ".venv" / "Scripts" / "python.exe"
    exe = str(venv) if venv.is_file() else sys.executable
    try:
        proc = subprocess.run(
            [exe, "-m", "core.judge_inbox"],
            cwd=JUDGE_PROJECT_DIR,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            shell=False,
            check=False,
            timeout=JUDGE_TIMEOUT_SEC,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.SubprocessError) as e:  # TimeoutExpired 포함
        log.warning("판정 입구 실행 실패(%s)", type(e).__name__)
        return False
    if proc.returncode != 0:
        log.warning("판정 입구 종료코드 %s", proc.returncode)
    return proc.returncode == 0


def _handle_sns_judge(adapter: Adapter, event: Event) -> None:
    """🔍 판정하기 — 카드를 «판정 중» 으로 고치고(버튼 제거) VS Code 판정 세션을 띄운다.

    «판정 중» 카드는 목록으로 쌓는다(A 를 누른 뒤 B 를 눌러도 완료 신호가 둘 다 고친다). 같은
    카드 두 번 누름은 «목록에 있나» 로 거른다. 실행 실패면 그 카드만 목록에서 빼고 원래대로
    (버튼 포함) 되돌린 뒤 sns_launch_fail 을 보낸다 — 다시 누를 수 있다.
    """
    mid = event.message_id
    if event.channel_role != SNS_ROLE or not isinstance(mid, int):
        return
    card = event.text.strip()
    judging = f"{card}\n{sns_inbox.JUDGING_LINE}" if card else sns_inbox.JUDGING_LINE
    with _sns_lock:
        cards = _pending_cards(_load_sns_state())
        if str(mid) in cards:
            log.info("SNS 판정 중복 누름 무시 card=%s", mid)
            return
        cards[str(mid)] = judging
        _set_pending_cards(cards, {str(mid): time.time()})
        # 같은 락 안에서 — 완료 신호가 등록과 편집 사이에 끼면 🎉 로 고친 카드를 이 편집이 다시
        # «판정 중» 으로 덮어, 목록 밖·버튼 없는 카드가 영구히 남는다.
        adapter.edit(event.channel_id, mid, judging)  # 즉시 — buttons 없음 = 버튼 제거
    # 입구 실행(최대 60초)은 데몬 스레드로 — 이벤트 워커를 막지 않는다.
    _sns_spawn("sns-judge", _launch_judge_for_card, adapter, event.channel_id, mid, card)


def _launch_judge_for_card(adapter: Adapter, channel_id: int, mid: int, card: str) -> None:
    """판정 입구 실행 — 실패면 그 카드만 목록에서 빼고 원래대로(버튼 포함) + sns_launch_fail."""
    if launch_judge():
        log.info("SNS 판정 세션 실행 card=%s", mid)
        return
    with _sns_lock:
        cards = _pending_cards(_load_sns_state())
        cards.pop(str(mid), None)
        _set_pending_cards(cards)
    adapter.edit(channel_id, mid, card, [SNS_JUDGE_BUTTON])
    adapter.send(channel_id, sns_inbox.LAUNCH_FAIL_TEXT)


def _valid_done(raw: object) -> TypeGuard[dict[str, Any]]:
    """완료 신호 스키마 — {"사용": int, "폐기": int, "시각": str(≤64자)}(bool 은 int 가 아니다)."""
    if not isinstance(raw, dict):
        return False
    counts = [raw.get("사용"), raw.get("폐기")]
    stamp = raw.get("시각")
    return (
        all(isinstance(n, int) and not isinstance(n, bool) and n >= 0 for n in counts)
        and isinstance(stamp, str)
        and len(stamp) <= _SNS_DONE_STAMP_MAXLEN
    )


def _judged(text: str) -> str:
    if sns_inbox.JUDGING_LINE in text:
        return text.replace(sns_inbox.JUDGING_LINE, sns_inbox.JUDGED_LINE)
    return f"{text}\n{sns_inbox.JUDGED_LINE}"


def check_sns_judge_done(adapter: Adapter) -> None:
    """완료 신호가 있으면 «판정 중» 카드 **전부**를 🎉 판정완료 로 고치고 신호 파일을 지운다.

    타이머 틱(_dispatch_loop)마다 부른다. 스키마가 틀리거나 32KB 를 넘으면 파일을 남기고 경고만
    (파일이 바뀔 때마다 1번) — 사람이 고치거나 세션이 다시 쓰면 잡힌다.
    `항목` 이 있으면 «😎 판정완료» 카드를 새 메시지로 보낸다(없으면 🎉 만 — 호환).
    """
    global _sns_done_warned, _sns_channel_warned
    try:
        st = SNS_DONE_FILE.stat()
    except FileNotFoundError:
        return
    raw: object = None
    if st.st_size <= _SNS_DONE_MAX_BYTES:  # 큰 파일은 읽지도 않는다
        try:
            raw = json.loads(SNS_DONE_FILE.read_text(encoding="utf-8-sig"))  # BOM 흡수
        except (OSError, ValueError):
            raw = None
    if not _valid_done(raw):
        if _sns_done_warned != (st.st_mtime, st.st_size):
            _sns_done_warned = (st.st_mtime, st.st_size)
            log.warning("sns_judge_done.json 스키마 오류 — 무시(파일 유지)")
        return
    use, drop = _judge_items(raw.get("항목"))
    if raw.get("항목") is not None and (raw["사용"], raw["폐기"]) != (len(use), len(drop)):
        # 카드는 항목 기준으로 낸다 — 숫자와 다르면 카드·로그가 다른 값을 말하니 흔적을 남긴다.
        log.warning(
            "완료 신호 숫자(사용=%d 폐기=%d) ≠ 항목(사용=%d 폐기=%d) — 카드는 항목 기준",
            raw["사용"],
            raw["폐기"],
            len(use),
            len(drop),
        )
    summary = sns_inbox.judge_cards(use, drop, _SNS_JUDGE_CARD_LIMIT)
    # 긴 수집(히스토리 최대 120초)이 락을 쥐고 있으면 기다리지 않고 다음 25초 틱에 다시 본다 —
    # 알림 타이머 스레드가 막히면 다른 예약 알림까지 밀린다.
    if not _sns_lock.acquire(blocking=False):
        log.info("SNS 완료 신호 — 수집 중이라 다음 틱에 처리")
        return
    try:
        cards = _pending_cards(_load_sns_state())
        channel = adapter.role_channel(SNS_ROLE) if cards or summary else None
        if (cards or summary) and channel is None:
            if _sns_channel_warned != (st.st_mtime, st.st_size):
                _sns_channel_warned = (st.st_mtime, st.st_size)
                log.warning("#%s 채널 미매핑 — 판정완료 표시 보류", SNS_ROLE)
            return
        for cid, text in cards.items():
            assert channel is not None  # 위 가드(cards 면 channel 이 있다) — mypy 좁히기
            adapter.edit(channel, int(cid), _judged(text))
        if cards:
            _set_pending_cards({})
        else:
            log.info("판정 완료 신호 — 대기 카드 없음")
        # 카드 전송 **전에** 신호를 지운다 — 전송이 실패해도 다음 틱이 카드를 두 번 보내지 않게.
        SNS_DONE_FILE.unlink(missing_ok=True)
    finally:
        _sns_lock.release()
    log.info(
        "SNS 판정 완료 카드=%d 사용=%d 폐기=%d 항목=%d",
        len(cards),
        raw["사용"],
        raw["폐기"],
        len(use) + len(drop),
    )
    # 🔴 칸 값은 SNS 원문을 요약한 외부 유래 문자열 — 어댑터 send 가 마스킹하고, 멘션은
    # 클라이언트 전역 allowed_mentions=none 이 막는다(@everyone·역할·유저 멘션 무발사).
    # 로그엔 건수만. 카드 수정은 휴대폰 알림이 안 와서 **새 메시지**로 보낸다(계획 §2).
    # 요약 → 사용 카드 순으로 동기 전송(send 가 끝나야 다음 장) — 순서가 보장된다. 디스코드
    # rate limit(채널당 5건/5초)은 discord.py 가 429 를 받아 기다렸다 보낸다(11장 ≈ 10초, 호출
    # 타임아웃 30초 안). 한 장이 실패해도 나머지는 계속 보내고, 실패 안내는 한 번만.
    gap = sns_inbox.CARD_GAP  # 카드 사이 한 줄(디스코드가 연속 메시지를 붙여 그린다)
    failed = sum(adapter.send(channel, card + gap) is None for card in summary) if channel else 0
    if failed:
        log.warning("판정완료 카드 %d/%d장 게시 실패", failed, len(summary))
        post_system_notice(adapter, digest_giveup_text(SNS_NOTIFY_ID))


def _judge_items(raw: object) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """완료 신호 `항목` → (사용, 폐기) 칸 목록. 정화(제어문자·개행 접기) + 칸당 200자 자르기.

    없음 = 빈 목록(호환). 모르는 판정·빠진 칸·문자열 아닌 값은 **그 항목만** 버리고 경고 1번.
    """
    if raw is None:
        return [], []
    if not isinstance(raw, list):
        log.warning("완료 신호 항목이 목록이 아님 — 판정완료 카드 생략")
        return [], []
    out: dict[str, list[dict[str, str]]] = {"사용": [], "폐기": []}
    bad = 0
    for item in raw:
        verdict = item.get("판정") if isinstance(item, dict) else None
        fields = _SNS_ITEM_FIELDS.get(verdict) if isinstance(verdict, str) else None
        if fields is None or not all(isinstance(item.get(f), str) for f in fields):
            bad += 1
            continue
        clean = {}
        for f in fields:
            value = strip_control_line(item[f])
            if len(value) > _SNS_FIELD_MAXLEN:
                value = value[:_SNS_FIELD_MAXLEN] + "…"
            # 생 URL 자동 링크·미리보기 차단 — `://` 사이에 폭0 공백(U+200B)을 넣는다. 보이는 글자는
            # 같다. 정화(strip_control_line)가 폭0 문자를 지우므로 반드시 그 **뒤**에 넣는다.
            clean[f] = _URL_SCHEME_RE.sub("\\1:\u200b//", value)
        out[item["판정"]].append(clean)
    if bad:
        log.warning("완료 신호 항목 %d건 형식 오류 — 그 항목만 버림", bad)
    return out["사용"], out["폐기"]


def _handle_text(adapter: Adapter, event: Event) -> None:
    """텍스트 메시지 처리 — 음악·청소·재시작·도움말 명령만. 그 밖의 평문은 무회신(로그만)."""
    channel_id = event.channel_id
    text = event.text
    # 플레이리스트 채널 게이트(최상단): 화이트리스트(ㅁ노래·정지·다음·청소·추가·삭제·재생·
    # 목록·스포티파이)만 통과.
    # 그 외(잡담·다른 ㅁ명령·순수 링크·빈 메시지)는 반응·안내 없이 조용히 무시한다.
    if event.channel_role in _MUSIC_ONLY_ROLES and not _is_playlist_command(text):
        return
    if text == "":
        # 어댑터가 비지원 메시지(스티커 등, text 키 없음)를 text="" 로 정규화 → 안내.
        adapter.send(channel_id, "텍스트 메시지만 처리합니다")
        return
    stripped = text.strip()

    # 음악 재생 명령('ㅁ노래'·'ㅁ정지'·'ㅁ다음'). 별칭 해석 이전에 둬야 한다 — 아래 cmd 분기의
    # `cmd.startswith("ㅁ") and cmd not in COMMANDS → HELP` 폴백으로 이 명령이 새는 것 방지.
    # 재생은 디스코드 음성 소관 → adapter capability 로 위임(코어는 판정만, clear_channel 패턴).
    # 🔴 빈 문자열 = 미발송(adapter.play/stop/skip_music 공통 계약) — 어댑터가 이미 보냈거나
    # 보낼 것이 없다는 뜻이다. 'ㅁ노래' 회신은 곡 전환 알림보다 **먼저** 나가야 해서 어댑터가
    # 재생 시작 전에 직접 보낸다(코어는 반환 뒤에야 send 할 수 있어 순서가 뒤집힌다).
    act = music_action(stripped)
    if act == "play":
        log.info("chat=%s cmd=music play", channel_id)
        if reply := adapter.play_music(channel_id, event.user_id):
            adapter.send(channel_id, reply)
        return
    if act == "stop":
        log.info("chat=%s cmd=music stop", channel_id)
        if reply := adapter.stop_music(channel_id):
            adapter.send(channel_id, reply)
        return
    if act == "skip":
        log.info("chat=%s cmd=music skip", channel_id)
        if reply := adapter.skip_music(channel_id):
            adapter.send(channel_id, reply)
        return

    # 'ㅁ목록' — 재생목록 전곡 조회(읽기 전용). 여러 메시지로 나눠 보낸다.
    # ㅁ삭제와 같이 인가 우회 대상이 **아니다**(_playlist_bypass 3조건 중 비용·회신 유계 위반) —
    # 여기 도달하는 비인가 user 는 없다. 이 분기는 허용목록 user 전용.
    if is_music_list(stripped):
        log.info("chat=%s cmd=music list", channel_id)
        _handle_music_list(adapter, channel_id)
        return

    # 'ㅁ스포티파이' — 주간차트 3개 상위 30곡씩을 재생목록에 일괄 추가(약 7분 소요).
    # ㅁ목록·ㅁ삭제와 같이 인가 우회 대상이 **아니다**(_playlist_bypass 3조건 중 1·2 위반) —
    # 여기 도달하는 비인가 user 는 없다. 이 분기는 허용목록 user 전용.
    if is_music_spotify(stripped):
        log.info("chat=%s cmd=music spotify", channel_id)
        # 찍을 달은 **여기서**(명령을 받은 시각) 확정한다 — 왜는 _mark_spotify_month 주석.
        _handle_music_spotify(adapter, channel_id, datetime.now(_KST).strftime("%Y-%m"))
        return

    # 'ㅁ추가 <링크|검색어>' — 유튜브 재생목록("코딩")에 추가. 접두 매칭이라 별칭 해석·help 폴백
    # (아래 `cmd.startswith("ㅁ") and cmd not in COMMANDS → HELP`)보다 앞에 둔다.
    if is_music_add(stripped):
        log.info("chat=%s cmd=music add", channel_id)
        _handle_music_add(adapter, channel_id, stripped)
        return

    # 'ㅁ삭제 <제목>' — 재생목록에서 제거(파괴적). 인가 우회 대상이 아니라 여기 도달하는 비인가
    # user 는 없다(_playlist_bypass 가 뺀다) — 이 분기는 허용목록 user 전용.
    if is_music_del(stripped):
        log.info("chat=%s cmd=music del", channel_id)
        _handle_music_del(adapter, channel_id, stripped)
        return

    # 'ㅁ재생 <제목>' — 그 곡을 지금 재생(큐에 없으면 어댑터가 유튜브 검색으로 폴백).
    # 재생 자체는 디스코드 음성 소관 → play_music(query=…) capability 로 위임(코어는 파싱·회신만).
    if is_music_play_one(stripped):
        log.info("chat=%s cmd=music play-one", channel_id)
        arg = _cmd_arg(stripped)
        if not arg:
            adapter.send(channel_id, "재생 실패(노래 제목 필요)")
        # 성공 회신은 ""(위 계약) — 곧 나갈 '💿 현재 재생 곡' 알림이 같은 곡을 이미 말한다.
        elif reply := adapter.play_music(channel_id, event.user_id, query=arg):
            adapter.send(channel_id, reply)
        return

    # 명령 동의어(ㅁ사용법)를 정규 ㅁ 토큰으로 접어 아래 분기가 한 경로만 알게 한다.
    # 슬래시·평문은 명령이 아니라 접힘 대상도 아니다.
    cmd = COMMAND_ALIASES.get(stripped) or stripped
    if cmd == "ㅁ도움말" or (cmd.startswith("ㅁ") and cmd not in COMMANDS):
        # ㅁ도움말·ㅁ사용법 + 알 수 없는 ㅁ… 명령의 폴백 = HELP.
        log.info("chat=%s cmd=help", channel_id)
        adapter.send(channel_id, HELP_TEXT)
        return
    if cmd == "ㅁ재시작":
        # 자기수정 루프 완결: 회신 먼저 보내 사용자에게 재시작을 알린 뒤 프로세스 종료(런처 재기동).
        log.info("chat=%s cmd=restart", channel_id)
        adapter.send(channel_id, "♻️ 재시작합니다…")
        _restart(adapter)
        return  # 도달하지 않음(_restart 가 exit) — 방어적
    if cmd == "ㅁ청소":
        # 파괴적: 바로 삭제하지 않고 확인 버튼을 거친다(clean:ok 탭 시 _handle_button 에서 실행).
        log.info("chat=%s cmd=clean 확인요청", channel_id)
        if event.channel_role == SNS_ROLE:
            # #SNS정보 — 🔗 링크청소(파랑) · 🧹 전체청소(빨강·파괴) · ✖ 취소(확인 메시지 삭제)
            adapter.send(
                channel_id,
                sns_inbox.SNS_CLEAN_CONFIRM,
                [
                    Button("🔗 링크청소", "clean:link", style="primary"),
                    Button("🧹 전체청소", "clean:all", style="danger"),
                    Button("✖ 취소", "clean:x", style="secondary"),
                ],
            )
        else:
            adapter.send(
                channel_id,
                "🧹 메시지를 청소할까요?",
                [Button("🧹 청소", "clean:ok", ""), Button("✖ 취소", "clean:x", "")],
            )
        return

    # 평문(프로젝트 원격 작업은 폐지) — 반응 없이 로그만.
    log.info("chat=%s 평문 무시(명령 아님)", channel_id)


def handle_event(adapter: Adapter, event: Event, *, allowed: frozenset[int]) -> None:
    """정규화 Event 통합 디스패처.

    인가 게이트(최우선): event.user_id 허용목록 대조 — 미허용은 무회신·로그만(§3.1). 단, 좁은
    예외 하나로 서버 멤버 누구나 쓰게 인가를 우회한다: 플레이리스트 채널의 화이트리스트 음악 명령
    (_playlist_bypass). 이후 kind 분기. 코어는 어댑터 계약 메서드만 호출한다
    (플랫폼 API 직접 호출 없음).
    """
    if not is_allowed(event.user_id, allowed) and not _playlist_bypass(event):
        log.warning("미허용 user_id=%s %s 무시", event.user_id, event.kind)
        return
    # 🔴 이벤트 처리 전체를 «바쁨» 으로 — 워커가 디스코드 코루틴(ㅁ스포티파이 90곡·ㅁ목록 여러
    # 페이지·음악 추가 등)을 기다리는 동안 자동 재시작이 어댑터를 닫으면 그 코루틴이
    # 이벤트 루프와 함께 끊긴다(«Task was destroyed but it is pending»). 핸들러 끝까지 기다린다.
    with _working():
        _dispatch_event(adapter, event)


def _dispatch_event(adapter: Adapter, event: Event) -> None:
    """인가를 통과한 이벤트의 kind 분기(handle_event 가 «바쁨» 안에서 부른다)."""
    # 프로젝트 채널(옛 `_Project/*` 자동생성분)은 더 이상 지시를 받지 않는다 — 무회신·로그만.
    if event.project is not None:
        log.info("chat=%s 프로젝트 채널 메시지 무시(%s)", event.channel_id, event.kind)
        return
    # #SNS정보 는 실시간 처리하지 않는다 — 공유 링크는 정오 러너가 히스토리로 읽는다(계획 §3-1).
    # 반응·안내 없이 조용히 무시(플레이리스트 게이트와 같은 태도). 버튼(🔍 판정하기·청소 확인)과
    # `ㅁ청소` 만 통과 — 청소는 확인 시점에 수집을 먼저 돌린다(_clean_sns_channel).
    if (
        event.channel_role == SNS_ROLE
        and event.kind != "button"
        and not (event.kind == "text" and event.text.strip() == "ㅁ청소")
    ):
        return
    if event.kind == "button":
        _handle_button(adapter, event)
    elif event.kind == "text":
        _handle_text(adapter, event)


# ══════════════════════════════════════════════════════════════════════════
# 메인 루프
# ══════════════════════════════════════════════════════════════════════════
def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _dispatch_loop(
    adapter: Adapter,
    stop: threading.Event,
) -> None:
    """알림 스케줄 주기 틱(§3.3) — poll 카데언스와 독립된 타이머 스레드. stop 시 즉시 종료.

    스케줄을 인자로 캐시하지 않는다 — dispatch_notifications 가 매 틱 notify.json 을 다시 읽어
    수동 편집이 재기동 없이 반영된다(핫리로드).
    """
    while not stop.wait(NOTIFY_TICK_SEC):
        try:
            dispatch_notifications(adapter)
        except Exception as e:  # 알림 발송 오류로 스레드가 죽지 않게(타입만 기록)
            log.error("알림 발송 중 예외: %s", type(e).__name__)
        try:
            check_sns_judge_done(adapter)  # 판정 완료 신호(계획의 «1분 주기» — 이 틱이 더 촘촘하다)
        except Exception as e:
            log.error("SNS 완료 신호 확인 중 예외: %s", type(e).__name__)


def main() -> int:
    setup_logging()
    if sys.version_info < (3, 12, 3):
        log.error(
            "Python 3.12.3+ 필요(현재 %s). 종료.",
            ".".join(map(str, sys.version_info[:3])),
        )
        return 1
    env = load_env(PROJECT_DIR / ".env")

    # 디스코드 전용. 봇 토큰·허용 유저 ID 는 .env 로만(커밋 금지).
    token = env.get("DISCORD_BOT_TOKEN", "").strip()
    allowed = parse_allowed(env.get("DISCORD_ALLOWED_USER_IDS", ""))
    if not token:
        log.error(".env 에 DISCORD_BOT_TOKEN 이(가) 없습니다. .env.example 참고.")
        return 1
    if not allowed:
        log.error(".env 에 DISCORD_ALLOWED_USER_IDS 가 없습니다(허용목록 필수). 종료.")
        return 1
    # 미국주식 다이제스트(뉴스 요약)와 로그인 만료 감지가 claude CLI 를 쓴다.
    if not shutil.which("claude"):
        log.error("claude CLI 를 PATH 에서 찾지 못했습니다.")
        return 1

    # 회신 마스킹 대상: 봇 토큰 + 내부 절대경로(사용자명) + .env 값 전부(다이제스트 유출 방어).
    secrets = build_secrets(token, REPO_ROOT, env)

    if not acquire_lock(PID_FILE):
        log.error("다른 브리지 인스턴스가 실행 중입니다(pidfile). 종료.")
        return 1

    schedules = load_schedules(SCHEDULES_FILE)
    notify_fired.update(load_notify_state(NOTIFY_STATE_FILE, datetime.now(_KST).date().isoformat()))
    global sns_allowed
    sns_allowed = allowed  # SNS 히스토리도 이벤트 경로와 같은 허용목록으로 거른다
    sns_init_state()  # 첫 기동이면 «지금» 을 수집 시작점으로(과거 백필 없음)

    # 지연 import: discord.py 는 discord_adapter 에만 격리 — 코어(bridge)를 직접 import 하는
    # 경로(단위 테스트)는 이 줄에 닿지 않아 discord.py 미설치 환경에서도 죽지 않는다
    # (본체 stdlib 전용 계약 유지 = 플랫폼 교체 seam).
    from discord_adapter import DiscordAdapter

    # 재생목록은 **ID 하나만** .env 에 둔다(MUSIC_PLAYLIST_ID). 종전엔 재생용 URL(.env
    # MUSIC_PLAYLIST_URL)과 추가용 ID(youtube.PLAYLIST_ID 상수)가 따로 있어, 둘이 어긋나면
    # 'ㅁ추가'로 넣은 곡이 'ㅁ노래' 재생목록에 안 나왔다 — .env.example 이 "같아야 한다"고
    # 경고를 달아 사람이 지키게 하던 자리다. ID 에서 URL 을 만들어 어긋날 수 없게 한다.
    playlist_id = env.get("MUSIC_PLAYLIST_ID", "").strip()
    playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}" if playlist_id else ""
    if playlist_id:
        # ponytail: 모듈 상수 대입. add_video 가 유일한 진입점이고 워커가 단일이라 이걸로 충분 —
        # 재생목록이 요청마다 달라지면 그때 인자로 넘긴다.
        youtube.PLAYLIST_ID = playlist_id

    adapter: Adapter = DiscordAdapter(
        token,
        secrets,
        allowed,
        channel_map_file=CHANNEL_MAP_FILE,
        music_playlist_url=playlist_url,
        quiet_boot=consume_reload_marker(),  # 자동 재시작 직후면 🟢 기동 알림 생략
    )
    set_login_alert(
        lambda: post_system_notice(adapter, f"{notice_stamp()} {LOGIN_EXPIRED_TEXT}")
    )  # 🔐 로그인 만료 알림
    log.info("브리지 시작(discord). allowed=%d개 알림=%d건", len(allowed), len(schedules))

    # 접속 성공 신호: on_ready 를 기다렸다 READY_FILE 을 만든다. «N초 뒤에도 살아 있으면 성공»
    # 타이머는 토큰 거부(실패가 4초쯤에 드러난다)를 STARTED 로 오보한다 — 시간을 늘리는 건
    # 땜질이라 성공 자체를 신호로 쓴다.
    READY_FILE.unlink(missing_ok=True)

    def _mark_ready() -> None:
        # wait_ready 는 Adapter 계약 밖 어댑터 훅이라 getattr 로 선택 호출(계약 표면 오염 방지).
        wait = getattr(adapter, "wait_ready", None)
        if callable(wait) and wait(60):
            READY_FILE.write_text("ready", encoding="utf-8")

    threading.Thread(target=_mark_ready, name="ready-marker", daemon=True).start()

    # ① 시각 알림: poll(Gateway 수신) 블록 중에도 발송되도록 독립 타이머 스레드로 구동(§3.3).
    stop = threading.Event()
    disp = threading.Thread(
        target=_dispatch_loop,
        args=(adapter, stop),
        name="dispatch",
        daemon=True,
    )
    disp.start()
    threading.Thread(
        target=_reload_loop,
        args=(adapter, stop, ReloadWatcher(PROJECT_DIR)),
        name="reload-watch",
        daemon=True,
    ).start()
    try:
        for event in adapter.poll():
            try:
                handle_event(adapter, event, allowed=allowed)
            except Exception as e:  # 한 이벤트 오류로 루프가 죽지 않게(타입만 기록)
                log.error("event 처리 중 예외: %s", type(e).__name__)
    except KeyboardInterrupt:
        log.info("종료 요청(Ctrl+C).")
    finally:
        stop.set()
        adapter.close()
        PID_FILE.unlink(missing_ok=True)
        READY_FILE.unlink(missing_ok=True)
    # 봇 스레드가 로그인 거부·게이트웨이 예외로 죽어 끝난 경우는 실패(1)다 — 종료코드가 정상 종료와
    # 구분돼야 런처·run_loop 가 재기동을 판단한다.
    if _reload_requested:
        return RELOAD_EXIT_CODE
    return 1 if getattr(adapter, "bot_failed", False) else 0


if __name__ == "__main__":
    if "--us-digest-dry-run" in sys.argv:
        # Windows 콘솔 기본 코드페이지(cp949)는 이모지를 못 찍어 print 가 죽는다 — 파일은 utf-8
        # 인데 stdout 때문에 리포트를 통째로 잃지 않게 여기서 콘솔만 utf-8 로 돌린다.
        _reconfigure = getattr(sys.stdout, "reconfigure", None)
        if callable(_reconfigure):
            _reconfigure(encoding="utf-8", errors="replace")
        # 봇을 띄우지 않는 진단 경로 — 로그는 stdout 으로만(라이브 봇이 쓰는 bridge.log 를
        # 같은 시각에 두 프로세스가 열지 않게).
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout
        )
        sys.exit(us_digest_dry_run(True if "--weekly" in sys.argv else None))
    else:
        sys.exit(main())
