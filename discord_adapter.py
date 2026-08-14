#!/usr/bin/env python3
"""discord_adapter.py — 디스코드 플랫폼 어댑터(Adapter 구현).

spike/discord_bridge_spike.py 로 실증한 패턴을 정식화한다: discord.py(asyncio) 봇을 전용
스레드의 이벤트루프에서 구동하고, on_message/on_interaction 이 정규화 `Event` 를 queue.Queue 에
적재하면 poll() 이 `.get()` 으로 직렬 소비한다(§2.4/§3.2). send/edit/ack 는 워커(동기) 스레드에서
`run_coroutine_threadsafe(coro, loop).result(timeout)` 로 코루틴 완료까지 블록해 동기 값을 반환한다.

의존성 격리: discord.py import 는 **이 파일에만** 있다. 코어(bridge.main)는 이 모듈을 지연
import 하므로 discord.py 미설치 환경에서도 순수 함수 경로(selftest·단위 테스트)가 죽지 않는다
(계약: 본체 stdlib 전용).

보안 경계(§2.4):
- fetch_file 은 디스코드 CDN 도메인 화이트리스트(cdn.discordapp.com/media.discordapp.net)·확장자·
  10MB·경로 트래버설 차단(basename 만)을 적용.
- custom_id 는 신뢰 경계 밖 — parse_callback 정확 매칭만(임의 실행 금지).
- 봇 토큰은 어댑터 인스턴스에만 보관, 전송 직전 mask_secrets 로 방어심층 마스킹.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import queue
import random
import re
import threading
import urllib.parse
import urllib.request
from collections.abc import Callable, Coroutine, Iterator
from pathlib import Path
from typing import Any

import discord  # 유일한 discord.py import 지점.
import imageio_ffmpeg  # 번들 ffmpeg 실행파일 경로(FFmpegPCMAudio executable).
import yt_dlp  # 재생목록·스트림 URL 추출(플랫폼 격리 — 이 파일에만).

# 콜백 코덱(parse_callback/encode_callback)·청킹·다운로드 가드는 플랫폼 무관 정본(§1.3·§2.4)이라
# 공유 base(adapter)에서 재사용한다 — adapter 는 stdlib 전용이라 여기서 import 해도 추가 런타임
# 의존이 생기지 않는다(순수 문자열/상수 재사용, 보안 로직 단일 소스 유지).
from adapter import (
    _NOREDIRECT_OPENER,
    MAX_PHOTO_BYTES,
    PHOTO_EXTS,
    Button,
    Event,
    chunk_text,
    encode_callback,
    mask_secrets,
    parse_callback,
)

# 코어 회신 헤더 상수(§4.1 색 판정 단일 소스) + 프로젝트 한글 라벨(채널명 표시용). 어댑터→코어
# 방향 import 지만 discord.py 를 코어로 끌어들이지 않는다(bridge 는 stdlib 전용·discord_adapter 를
# top-level import 안 함 — 순환 없음). PROJECT_LABELS 는 채널 표시명, channel_map 값은 폴더명 원문.
from bridge import (
    HEADER_CHOICE,
    HEADER_DONE,
    HEADER_FAIL,
    HEADER_NOTE,
    PROJECT_LABELS,
    STATUS_LEADERS,
)

log = logging.getLogger("bridge")

DISCORD_LIMIT = 2000  # 디스코드 메시지 한도(§2.1) — 초과 장문은 청킹.
_CUSTOM_ID_LIMIT = 100  # 디스코드 custom_id 한도(§1.3). 우리 콜백 문자열은 항상 이 안(id·name≤64).
_CALL_TIMEOUT = 30  # run_coroutine_threadsafe().result() 타임아웃(§3.2).
# fetch_file 다운로드 도메인 고정(§2.4 — 임의 URL 다운로드=SSRF 차단).
_DISCORD_CDN_HOSTS = frozenset({"cdn.discordapp.com", "media.discordapp.net"})

# ── 상태색 매핑(§4.1 — 어댑터가 text 헤더로 판정, 계약 무변경) ──────────────────
# ⚠️ 동기화 주의(§4 주의점 1): 색 판정은 코어(bridge)의 회신/진행 헤더에 묶여 있다.
#   · 완료/실패/확인 3종은 위 HEADER_* import 로 문자열을 자동 추종(헤더 텍스트가 바뀌어도 무영향).
#   · 진행/예약알림 선두 이모지도 코어 STATUS_LEADERS import(단일 소스) — 코어 변경 자동 추종.
_COLOR_DONE = 0x3ECF85  # 초록 — 처리완료
_COLOR_FAIL = 0xF0565B  # 빨강 — 처리실패
_COLOR_INFO = 0x5865F2  # 블러플 — 추가 확인사항 / push 승인 대기
_COLOR_WAIT = 0xEEBB4D  # 노랑 — 진행 중 / 예약 알림
_EMBED_TITLE_LIMIT = 256  # discord Embed title 한도
_EMBED_DESC_LIMIT = 4096  # discord Embed description 한도(§4.1 — 초과분은 후속 plain 청크)
# 카드 렌더(§card 규약) 전용 한도 — 초과분은 자른다(디스코드가 400 으로 거부하므로).
_EMBED_AUTHOR_LIMIT = 256
_EMBED_FIELD_NAME_LIMIT = 256
_EMBED_FIELD_VALUE_LIMIT = 1024
_EMBED_FOOTER_LIMIT = 2048
_EMBED_FIELDS_MAX = 25
# 임베드 **전 슬롯 합계** 한도(디스코드). 넘으면 400 으로 메시지 자체가 안 나간다 — 슬롯별
# 한도를 다 지켜도 걸릴 수 있어 누적으로도 센다(build_card_embed).
_EMBED_TOTAL_LIMIT = 6000
_OMIT_NOTE_MAXLEN = 40  # footer 에 붙일 `⚠️N개 필드 생략(길이 초과)` 몫 — 총합에서 미리 뺀다

# ── ①(채널 자동생성 §4.4) — 서버 구조 ───────────────────────────────────────
# 카테고리명(대소문자·공백 보존). 특수 채널 = (표시명, kind, role tag). 프로젝트 카테고리는 setup 시
# 폴더명으로 채운다. 텍스트 채널명 = 라벨의 공백·하이픈 제거 붙여쓰기(디스코드 ASCII 소문자화).
# 재사용=channelID(맵) 1차·_canon 2차, 리네임 판단=**정확 이름 비교**(_canon collapse 함정 회피).
# 봇 서버 닉네임(on_ready 마다 멱등 고정 — 강퇴/재초대 시 수동 닉이 풀려서). 여기만 바꾸면 끝.
_BOT_NICKNAME = "치이카와 봇"
# 카테고리 표시명(이모지·공백·대문자 자유). 기존 카테고리는 코어명(_cat_core: 이모지·기호·공백 제거)
# 으로 탐색 후 이모지형으로 rename(중복 생성 방지·멱등). 음성 카테고리는 이전 이름 '음성'도 별칭.
_CAT_SIMPLE = "🗂️ 간단처리"
_CAT_PROJECT = "📁 프로젝트"
_CAT_DATA = "📊 데이터분석"
_CAT_SCHED = "🗓️ 스케쥴러"
_CAT_SYSTEM = "⚙️ 시스템"
_CAT_QUESTION = "❓ 질문"  # 게스트질문(개발자 외 서버 멤버 Q&A) 전용 — 시스템↔PlayList 사이.
_CAT_VOICE = "🎵 PlayList"
# 위치 순서(0..6): 데이터분석 아래·시스템 위에 스케쥴러, 시스템 아래·PlayList 위에 질문.
_CAT_ORDER = [
    _CAT_SIMPLE,
    _CAT_PROJECT,
    _CAT_DATA,
    _CAT_SCHED,
    _CAT_SYSTEM,
    _CAT_QUESTION,
    _CAT_VOICE,
]
# 카테고리별 코어 별칭(기존 탐색) — 대부분 코어명 1개, 음성은 이전 이름 '음성' 포함.
_CAT_ALIASES: dict[str, list[str]] = {
    _CAT_SIMPLE: ["간단처리"],
    _CAT_PROJECT: ["프로젝트"],
    _CAT_DATA: ["데이터분석"],
    _CAT_SCHED: ["스케쥴러"],
    _CAT_SYSTEM: ["시스템"],
    _CAT_QUESTION: ["질문"],
    _CAT_VOICE: ["PlayList", "음성"],
}
_VOICE_NAME = "PlayList"  # 음성 채널(재생 기능은 후속 — 자리만). 기본 음성 '일반'을 리네임/생성.
_DEFAULT_GENERAL = ("일반", "general")  # 디스코드 기본 텍스트/음성 채널명(로컬라이즈 포함)
# 디스코드 기본 빈 카테고리 — 비어있고 봇이 만든 게 아닐 때만 삭제(#5).
_DEFAULT_CATEGORIES = ("채팅 채널", "Text Channels", "음성 채널", "Voice Channels")
_DATA_TOPIC = "HTML 리포트는 디스코드에 안 뜸 — 파일/요약만."  # 데이터분석 채널 토픽(안내 1회)
# ponytail: 빈이름 targeting 폐기 — 디스코드가 U+3164·U+2800 둘 다 400 거부(라이브 실측). 두 채널
# (간단처리 텍스트·PlayList 음성)은 정상 표시명을 갖고 일반 리네임 경로(정확명 비교·멱등)를 탄다.
_SPECIAL: dict[str, list[tuple[str, str, str]]] = {
    _CAT_SIMPLE: [("간단처리", "role", "간단처리")],  # 개발자 전용·full 도구
    # 게스트질문(개발자 외 서버 멤버·웹검색 Q&A·cwd 격리·코어 bridge._GUEST_ROLE 와 태그 일치)은
    # 별도 '질문' 카테고리. role tag/채널명 불변(격리 매칭 유지) — 카테고리 배치만 분리.
    _CAT_QUESTION: [("게스트질문", "role", "게스트질문")],
    _CAT_DATA: [("데이터분석", "role", "데이터분석")],  # 하이픈 원천 제거
    # ⚠️ **표시명과 tag 가 일부러 다르다 — 맞추려고 tag 를 고치지 마라.** 채널 탐색은
    # ①`channel_map.json` 의 `(kind, tag)` → ②이름 canon 순인데, tag 를 바꾸면 1·2차 모두
    # 빗나가 **새 채널이 생긴다**. 옛 채널의 히스토리도, 손으로 설정한 읽기전용 권한도 새
    # 채널엔 없다. 표시명만 바꾸면 `_rename_if_needed` 가 **기존 채널을 제자리에서 rename**
    # 한다(히스토리·권한 보존). `_READONLY_TAGS`·`notify.json` 의 `channel` 도 tag 기준이라
    # 함께 무변경. 같은 규칙의 선례 = 위 게스트질문("role tag/채널명 불변").
    # 2026-08-02: 카드가 미국장 7 + 국내장 3 이 되어 표시명 `미국주식` → `반도체주식`.
    # 2026-08-13: 유튜브 노트 파이프라인 채널 3개 추가(개발자 지시). 순서도 개발자 지정.
    #   표시명은 **하이픈 제거 후 실제로 보일 이름을 그대로** 적는다 — 위 98줄대로 채널명에서
    #   하이픈이 지워지므로 `유튜브-Dev` 라 써도 `#유튜브dev` 로 뜬다. 헷갈리지 않게 결과를 적었다.
    #   tag 는 **레포·드라이브 폴더명과 일치**시켜 `notify.json` 의 `channel` 값과 갈리지 않게 한다
    #   (tag 는 절대 고치지 말 것 — 위 경고. 표시명만 바꾸면 기존 채널이 제자리 rename 된다).
    _CAT_SCHED: [
        ("반도체주식", "role", "미국주식"),
        ("오픈소스", "role", "오픈소스"),
        ("유튜브dev", "role", "유튜브-Dev"),
        ("유튜브일반", "role", "유튜브-일반"),
        # ⚠️ `개발자료` 는 2026-08-15 제거(개발자 지시) — 채널·드라이브 폴더 둘 다 삭제했다.
        # **순서는 «디스코드 채널 삭제 → 이 목록에서 제거»** 다. 반대로 하면 재기동 한 번에
        # 고아 채널이 남고, 그 채널에 온 메시지는 미매핑이라 **채널명이 프로젝트명으로 해석된다**.
        # **이 목록에서 지우는 것이 삭제의 일부다** — 남겨두면 다음 재기동의 채널 자동생성이
        # 조용히 되살린다(지운 사람은 지웠다고 믿는데 이틀 뒤 다시 있다).
    ],
    _CAT_SYSTEM: [("알림", "role", "알림"), ("봇상태", "role", "봇상태")],  # 하이픈 원천 제거
}
# 사람이 글을 못 쓰는 채널(tag 기준 — 표시명 `#반도체주식`). **봇이 카드를 밀어넣기만 하고
# 왕복이 없는 곳만** 넣는다 — `#오픈소스` 는 📌 버튼 상호작용이 있어 대상이 아니다
# (사용자: "매일 보는 용도로만 사용").
_READONLY_TAGS = frozenset({"미국주식"})
# 프로젝트 채널 내부 정본 순서(폴더명). 목록에 없는 프로젝트는 뒤로. h_* 를 맨 위(개발자 확정).
_PROJECT_ORDER = [
    "pdf_restyler",
    "H_security_sheet",
    "trading_info",
    "etf_info",
    "mobi_barter",
    "chiikawa_office",
    "claude_bridge",
]


def _canon(name: str) -> str:
    """채널명 정규화(재사용 대조) — 소문자 + 언더스코어/하이픈/공백 제거로 디스코드 변형에 견고."""
    return name.lower().replace("_", "").replace("-", "").replace(" ", "")


def _cat_core(name: str) -> str:
    """카테고리 코어명 — 이모지·기호·공백 제거, 단어(한글/영숫자)만 소문자화(기존 탐색·멱등)."""
    return re.sub(r"[^0-9A-Za-z가-힣]", "", name).lower()


def _desired_name(display: str) -> str:
    """텍스트 채널 저장명(붙여쓰기): 공백·하이픈 제거 + ASCII 소문자화(디스코드 저장형과 일치)."""
    return display.replace(" ", "").replace("-", "").lower()


def _project_order(names: list[str]) -> list[str]:
    """프로젝트 폴더명을 _PROJECT_ORDER 정본 순서로. 목록에 없는 건 뒤(안정 정렬)."""
    idx = {n: i for i, n in enumerate(_PROJECT_ORDER)}
    return sorted(names, key=lambda n: idx.get(n, len(_PROJECT_ORDER)))


def load_channel_map(path: Path) -> dict[int, tuple[str, str]]:
    """logs/channel_map.json → {channel_id: (kind, tag)}. 없음·손상은 빈 dict(방어적)."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out: dict[int, tuple[str, str]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, list) and len(v) == 2 and all(isinstance(x, str) for x in v):
                with contextlib.suppress(ValueError):
                    out[int(k)] = (v[0], v[1])
    return out


def save_channel_map(path: Path, cmap: dict[int, tuple[str, str]]) -> None:
    """channel_map 원자적 영속(tmp→replace). {channel_id: (kind, tag)} → {"<id>": [kind, tag]}."""
    payload = {str(cid): [kind, tag] for cid, (kind, tag) in cmap.items()}
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _style(style: str) -> Any:
    """Button.style → discord.ButtonStyle(§4.7: success→초록·primary→블루·danger→레드·그외 회색)."""
    return {
        "default": discord.ButtonStyle.secondary,
        "secondary": discord.ButtonStyle.secondary,
        "primary": discord.ButtonStyle.primary,
        "success": discord.ButtonStyle.success,
        "danger": discord.ButtonStyle.danger,
    }.get(style, discord.ButtonStyle.secondary)


def _status_color(text: str) -> int | None:
    """text 헤더로 상태색 판정(§4.1). 매칭 안 되면 None(=plain 마크다운 경로, 기존 무변경).

    완료/실패/확인 헤더는 접두 일치, 진행/알림은 선두 이모지. 목록·도움말·짧은 회신은 어디에도
    안 걸려 None → plain(디스코드에서 마크다운 렌더).
    """
    for head, col in (
        (HEADER_DONE, _COLOR_DONE),
        (HEADER_FAIL, _COLOR_FAIL),
        (HEADER_NOTE, _COLOR_INFO),
        (HEADER_CHOICE, _COLOR_INFO),  # 선택 질문 = '입력 대기'(push 승인과 같은 블러플)
    ):
        if text.startswith(head):
            return col
    if text[:1] in STATUS_LEADERS:
        return _COLOR_WAIT
    return None


def _build_embed(text: str, color: int) -> tuple[Any, str]:
    """상태 텍스트 → (discord.Embed, desc 4096 초과 오버플로 str). 헤더=첫 줄, 본문=나머지(§4.1).

    title = 첫 줄에서 대괄호 껍질을 벗긴 상태 요약, desc = 본문. desc 4096 초과분은 반환해
    호출측이 후속 plain 청크로 흘려보낸다(§2.2 오버플로 재사용). author 라인은 없다(봇 계정명이
    이미 상단에 뜨므로 중복 — 제거). 3열 필드·footer 소요시간은 범위 밖(주의점10).
    """
    first, _, rest = text.partition("\n")
    title = first.strip().lstrip("[").rstrip("]").strip()
    body = rest.strip()
    embed = discord.Embed(
        color=color,
        title=title[:_EMBED_TITLE_LIMIT] or None,
        description=body[:_EMBED_DESC_LIMIT] or None,
    )
    return embed, body[_EMBED_DESC_LIMIT:]


def build_card_embed(card: dict[str, Any], secrets: list[str]) -> Any:
    """카드 스펙(코어 dict, adapter.py 「Card 규약」) → discord.Embed. 다이제스트 카드 렌더 경로.

    **공용 `_build_embed`(진행·완료·실패·확인)와 분리된 별도 경로**다 — 일상 회신 렌더에 카드
    분기를 섞으면 그쪽이 깨진다. 문자열 슬롯은 전부 마스킹(§2.1 방어심층: 카드도 send/edit 과
    같은 문을 지난다)하고 디스코드 한도로 자른다. 색은 코어가 판정별로 정해 넘긴다.

    **총합 가드는 필수다.** 호출자가 둘이고 산식이 서로 다르다:
    ① 오픈소스 다이제스트 = `DIGEST_MAX_CARDS`(5) x `DIGEST_CARD_MAXLEN`(1000) + 제목·footer ≈ 5.1KB
    ② 미국주식 다이제스트(#반도체주식) = 필드 10 x `us_digest.FIELD_MAXLEN`(550) + 필드명·제목
       ·footer ≈ 5.7KB
    ②는 6000 까지 여유가 300자 남짓이라 **필드를 하나 늘리는 순간 400 으로 메시지가 통째로
    사라진다**(슬롯별 한도는 전부 지켜도 걸린다). 넘치는 필드는 버리되 **footer 에 `⚠️N개 생략`
    을 남긴다** — 한 필드를 잃는 편이 카드 전체를 잃는 것보다 낫지만, 조용히 잃으면 읽는 사람은
    그 카드의 마지막 필드들(국내장 지수·메모리·장비소재)이 사라진 걸 모른다. 필드를 늘릴 때
    `us_digest.FIELD_MAXLEN` 을 같이 낮추라는 계약이 그 상수 주석에 있다.
    description 도 남은 예산으로 한 번 더 자른다 — 슬롯별 한도만 지킨 조합(256+4096+256+2048)은
    **필드가 0개여도** 6000 을 넘는다.
    """

    def m(v: object, limit: int) -> str:
        return mask_secrets(str(v), secrets)[:limit]

    title = m(card.get("title") or "", _EMBED_TITLE_LIMIT)
    author = m(card.get("author") or "", _EMBED_AUTHOR_LIMIT)
    footer = m(card.get("footer") or "", _EMBED_FOOTER_LIMIT)
    budget = _EMBED_TOTAL_LIMIT - _OMIT_NOTE_MAXLEN  # 생략 안내 몫을 미리 뺀다
    fixed = len(title) + len(author) + len(footer)
    description = m(card.get("description") or "", min(_EMBED_DESC_LIMIT, max(0, budget - fixed)))
    embed = discord.Embed(
        color=int(card.get("color") or _COLOR_INFO),
        title=title or None,
        description=description or None,
    )
    if author:
        embed.set_author(name=author)
    total = fixed + len(description)
    entries = list(card.get("fields") or [])[:_EMBED_FIELDS_MAX]
    omitted = 0
    for i, (name, value, inline) in enumerate(entries):
        # 이름·값이 빈 필드는 디스코드가 400 으로 거부한다 — 코어가 안 넣지만 여기서도 막는다.
        fname, fvalue = m(name, _EMBED_FIELD_NAME_LIMIT), m(value, _EMBED_FIELD_VALUE_LIMIT)
        if not (fname and fvalue):
            continue
        if total + len(fname) + len(fvalue) > budget:
            omitted = len(entries) - i
            log.warning("카드 임베드 총합 초과 — 이후 %d개 필드 생략(%s)", omitted, fname[:40])
            break
        embed.add_field(name=fname, value=fvalue, inline=bool(inline))
        total += len(fname) + len(fvalue)
    if omitted:
        footer = f"⚠️{omitted}개 필드 생략(길이 초과)" + (f" · {footer}" if footer else "")
    if footer:
        embed.set_footer(text=footer[:_EMBED_FOOTER_LIMIT])
    return embed


def _send_kwargs(payload: Any, view: Any) -> dict[str, Any]:
    """발송 파트(plain str | discord.Embed) → channel.send kwargs. 버튼은 view 로 부착."""
    kwargs: dict[str, Any] = (
        {"embed": payload} if isinstance(payload, discord.Embed) else {"content": payload}
    )
    if view is not None:
        kwargs["view"] = view
    return kwargs


def render_view(buttons: list[Button]) -> Any:
    """list[Button] → discord.ui.View. custom_id=encode_callback(§1.3), 스타일 매핑, ≤100자.

    클릭은 클라이언트 레벨 on_interaction 이 custom_id 로 라우팅한다(뷰 자체 콜백 미사용) —
    비영속 custom_id(메시지 id·프로젝트명 포함)라 persistent view 등록 없이 전역 이벤트로 받는다.
    timeout=None: 뷰가 만료돼도 on_interaction 은 계속 발화하므로 렌더 목적상 무기한 유지해도 무해.
    """
    view = discord.ui.View(timeout=None)
    for b in buttons:
        # custom_id 는 char 기준 100자 캡(우리 값은 항상 그 안). 초과 시 잘리면 parse_callback 이
        # 거르므로(오작동 대신 무시) 안전(char 기준 100자 캡).
        cid = encode_callback(b.action, b.arg)[:_CUSTOM_ID_LIMIT]
        view.add_item(discord.ui.Button(label=b.label, style=_style(b.style), custom_id=cid))
    return view


def _is_vertical_list(buttons: list[Button] | None) -> bool:
    """버튼 묶음을 세로 1열 V2(LayoutView) 로 렌더할까 — 프로젝트 목록(p:)·선택지(c:) 전용.

    둘 다 항목 수가 가변(프로젝트 7+·선택지 N+직접입력)이라 classic View 5행 한도를 넘을 수 있어
    세로 V2 로 편다. push/취소/알림/오라클 등 고정 소수 버튼은 현행 가로(classic) 유지(계약 무변경).
    """
    return buttons is not None and len(buttons) > 0 and all(b.action in {"p", "c"} for b in buttons)


def render_project_view(header: str, buttons: list[Button]) -> Any:
    """프로젝트 목록 → Components V2 LayoutView(세로 1열). 헤더는 TextDisplay 로 흡수(content 불가).

    각 프로젝트를 ActionRow 1개(버튼 1개)로 쌓아 폰에서 한 줄에 하나씩 보이게 한다(실측 요구).
    classic View 5행 한도를 피하려 V2 를 쓴다(2.7.1 실측 동작). ponytail: V2 컴포넌트 40개 상한
    이라 헤더+2N ≤ 40 → 프로젝트 ~19개까지. 그 이상이면 페이징 필요(현재 7개 안팎이라 여유).
    """
    view = discord.ui.LayoutView(timeout=None)
    if header:
        view.add_item(discord.ui.TextDisplay(header))
    for b in buttons:
        cid = encode_callback(b.action, b.arg)[:_CUSTOM_ID_LIMIT]
        row = discord.ui.ActionRow()
        row.add_item(discord.ui.Button(label=b.label, style=_style(b.style), custom_id=cid))
        view.add_item(row)
    return view


class DiscordAdapter:
    """디스코드 Gateway/REST 를 Adapter 계약(poll·send·edit·ack·fetch_file·close)으로 감싼다.

    생성 시 봇토큰 + secrets(마스킹 대상) + allowed(선-필터용)를 주입받는다. 봇 스레드는 최초
    poll() 호출 때 기동한다(생성만으로는 접속하지 않음 — 단위 테스트에서 순수 메서드 검증 가능).
    """

    def __init__(
        self,
        token: str,
        secrets: list[str],
        allowed: frozenset[int],
        *,
        limit: int = DISCORD_LIMIT,
        channel_map_file: Path | None = None,
        music_playlist_url: str = "",
    ) -> None:
        self.token = token
        self.secrets = secrets
        # 어댑터 선-필터(방어심층·스팸/맵누증 차단). 권위 인가 게이트는 코어(handle_event, §3.1).
        self._allowed = allowed
        self.limit = limit
        self._queue: queue.Queue[Event | None] = queue.Queue()
        # callback_id -> live Interaction. ack(defer 이후 followup)로 잇는다. ack 에서 pop(정리).
        # ponytail: ack 가 항상 소비 + 비허용은 선-필터로 미적재라 맵 유계(토큰 15분 만료가 상한).
        self._interactions: dict[str, Any] = {}
        self._closed = False
        self.bot_failed = False  # 봇 스레드가 로그인·게이트웨이 예외로 죽었는가(종료코드 판정용)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        # Gateway 접속 완료(on_ready) 신호 — 재시작 복귀 통지 등 접속 후 send 를 여기서 기다린다.
        self._ready = threading.Event()
        # F1: on_ready 는 재접속마다 재발화 → 첫 셋업 중 겹치면 둘 다 옛 맵으로 시작해 중복 생성.
        # 이 락으로 _ensure_channels 를 직렬화(둘째는 첫째의 channel_map 커밋 후 진입 → 기존 발견).
        self._setup_lock = asyncio.Lock()
        # ①(채널 자동생성): channelID→(kind,tag) 매핑(영속). setup_channels 로 프로젝트명 주입,
        # on_ready 에서 _ensure_channels 가 생성·재사용·매핑한다. 파일 없으면 메모리만(테스트).
        self._channel_map_file = channel_map_file
        self._channel_map: dict[int, tuple[str, str]] = (
            load_channel_map(channel_map_file) if channel_map_file else {}
        )
        self._project_names: list[str] = []
        # 세로 목록으로 발송한 V2 메시지 id — Discord 는 IS_COMPONENTS_V2 flag 를 생성 시 고정해
        # edit 로 classic 전환을 막는다. 그래서 이 id 들은 edit 도 V2 로 유지한다(선택지 갱신·제거).
        # ponytail: 세션당 선택지/목록 수만큼 정수가 쌓이나(개인 봇·재시작 소멸) 무해 — 커지면 정리.
        self._v2_messages: set[int] = set()
        # ── 음악 재생('ㅁ노래') 상태(디스코드 음성 capability, 이 어댑터에만) ──────────────
        # 재생목록 URL 은 고정(.env MUSIC_PLAYLIST_URL). ffmpeg 는 imageio_ffmpeg 번들 실행파일.
        # 재생은 단일 길드·단일 음성연결이라 굵은 인스턴스 상태로 충분(봇 1대·동시 재생 1건).
        # ponytail: 단일 재생 세션 가정 — 다중 길드 동시재생이 필요해지면 길드별 상태 맵으로.
        self._music_playlist_url = music_playlist_url
        self._ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        self._voice: Any = None  # discord.VoiceClient(재생 중일 때만)
        self._music_entries: list[dict[str, Any]] = []  # 셔플된 flat 재생목록 엔트리
        self._music_index = 0
        self._music_stopping = False  # 정지 중 플래그(_after 가 advance 하지 않게)
        self._music_text_ch = 0  # 재생 시작 채널(로그·향후 통지용)
        intents = discord.Intents.default()
        intents.message_content = True  # on_message 본문 수신 필수(Developer Portal 도 켜야 함).
        self._client = discord.Client(intents=intents)
        self._register_events()

    def wait_ready(self, timeout: float = 30) -> bool:
        """Gateway on_ready 까지 대기(접속 후 send 안전 타이밍). Adapter 계약 밖 디스코드 훅."""
        return self._ready.wait(timeout)

    def setup_channels(self, project_names: list[str]) -> None:
        """①: 프로젝트 채널 목록을 주입(on_ready 의 _ensure_channels 가 사용). 생성은 접속 후."""
        self._project_names = list(project_names)

    def role_channel(self, role: str) -> int | None:
        """특수 채널 역할("알림"|"봇상태"|…) → channelID(channel_map 역조회). 없으면 None."""
        for cid, (kind, tag) in self._channel_map.items():
            if kind == "role" and tag == role:
                return cid
        return None

    def project_channel(self, project: str) -> int | None:
        """프로젝트 폴더명 → 프로젝트 채널 channelID(channel_map 역조회). 없으면 None."""
        for cid, (kind, tag) in self._channel_map.items():
            if kind == "project" and tag == project:
                return cid
        return None

    def clear_channel(self, channel_id: int) -> int:
        """채널 메시지 전체 삭제(purge) 후 삭제 건수. 파괴적 — 코어가 확인 버튼 뒤에만 호출.

        _run 이 루프 미준비·예외를 삼켜 None 을 줄 수 있으므로 int 아니면 0(안전 폴백).
        """
        result = self._run(self._purge_coro(channel_id))
        return result if isinstance(result, int) else 0

    # ── 음악 재생('ㅁ노래'·'ㅁ정지'·'ㅁ다음') capability(디스코드 음성) ──────────────
    def play_music(self, channel_id: int, user_id: int) -> str:
        """호출자(user_id)가 있는 음성채널에서 고정 재생목록을 셔플·반복 재생. 회신 문자열 반환.
        _run 이 루프 미준비·예외를 삼켜 None 을 줄 수 있으므로 str 아니면 안내 폴백.
        """
        r = self._run(self._music_play_coro(channel_id, user_id), timeout=60)
        return r if isinstance(r, str) else "⚠️ 음악 재생 중 오류가 발생했습니다."

    def stop_music(self, channel_id: int) -> str:  # noqa: ARG002 (Protocol 계약 시그니처)
        """재생 정지 + 음성채널 퇴장. 회신 문자열 반환(폴백은 안내). 단일 세션이라 채널 무관."""
        r = self._run(self._music_stop_coro(), timeout=60)
        return r if isinstance(r, str) else "⚠️ 음악 재생 중 오류가 발생했습니다."

    def skip_music(self, channel_id: int) -> str:  # noqa: ARG002 (Protocol 계약 시그니처)
        """현재 곡 건너뛰기(→ after 가 다음 곡 재생). 회신 문자열 반환(폴백은 안내). 채널 무관."""
        r = self._run(self._music_skip_coro(), timeout=60)
        return r if isinstance(r, str) else "⚠️ 음악 재생 중 오류가 발생했습니다."

    def search_video(self, query: str) -> tuple[str, str] | None:
        """'ㅁ추가 <검색어>' → yt-dlp ytsearch1 첫 결과 (videoId, 제목). 무결과·실패는 None.

        음성 재생(_run 경유 이벤트루프)과 달리 순수 메타 추출이라 워커 스레드에서 직접(블로킹)
        실행한다 — voice client·on_ready 무관. 익명·무료(YouTube Data API search.list 미사용).
        """
        try:
            opts = {"quiet": True, "skip_download": True, "extract_flat": "in_playlist"}
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(f"ytsearch1:{query}", download=False)
        except Exception as e:  # yt-dlp 추출 실패(네트워크·차단·무결과 파싱 등) — None 폴백
            log.warning("유튜브 검색 실패: %s", type(e).__name__)
            return None
        entries = (info or {}).get("entries") or []
        if not entries or not isinstance(entries[0], dict):
            return None
        vid = entries[0].get("id")
        if not vid:
            return None
        return (str(vid), str(entries[0].get("title") or vid))

    def enqueue_video(self, video_id: str, title: str) -> int:
        """재생 중이면 새 곡을 현재 곡 바로 뒤에 실시간 편입하고 편입 후 큐 곡수를 반환.

        재생 중이 아니면 no-op 0 — 다음 ㅁ노래에 자연 포함된다. _run 으로 이벤트루프에 직렬화해
        _advance/_play_current 의 _music_entries 접근과 경쟁 없이 삽입한다(기존 음악 패턴). _run 이
        실패(루프 미준비 등)로 None 을 주면 편입 실패로 보고 0.
        """
        r = self._run(self._enqueue_coro(video_id, title))
        return r if isinstance(r, int) else 0

    async def _enqueue_coro(self, video_id: str, title: str) -> int:
        if self._voice is None or not self._voice.is_connected() or not self._music_entries:
            return 0
        if self._music_stopping:
            return 0
        # FIFO: 현재 곡 뒤에 이미 쌓인 대기곡("queued" 표시)들을 건너뛴 첫 위치에 삽입 → 추가곡이
        # 추가순서대로 현재 곡 뒤에 이어진다(현재곡 → A → B → C → 기존 셔플). 재생돼서 _music_index
        # 가 지나가면 대기 블록에서 자연히 빠져(스캔은 index+1 부터라) _advance·재셔플 수정 불필요.
        # queued 키는 표식일 뿐 — _extract_stream/_play_current 은 id/title 만 써서 무영향.
        pos = self._music_index + 1
        while pos < len(self._music_entries) and self._music_entries[pos].get("queued"):
            pos += 1
        self._music_entries.insert(pos, {"id": video_id, "title": title, "queued": True})
        log.info("재생 큐 편입: id=%s pos=%d/%d", video_id, pos, len(self._music_entries))
        return len(self._music_entries)

    def _caller_voice_channel(self, user_id: int) -> Any:
        """호출자(user_id)가 접속해 있는 음성채널을 첫 길드에서 찾는다(없으면 None).

        voice_states 는 default 인텐트라 members 특권 인텐트 없이 조회 가능(각 채널의 members).
        """
        guild = self._client.guilds[0] if self._client.guilds else None
        if guild is None:
            return None
        return next(
            (v for v in guild.voice_channels if any(m.id == user_id for m in v.members)),
            None,
        )

    def _extract_flat(self) -> list[dict[str, Any]]:
        """재생목록을 flat 추출(곡별 스트림 URL 은 재생 직전 지연 해석). 매 재생 시 재로드."""
        opts = {"extract_flat": "in_playlist", "quiet": True, "skip_download": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(self._music_playlist_url, download=False)
        entries = (info or {}).get("entries") or []
        return [e for e in entries if isinstance(e, dict)]

    def _extract_stream(self, entry: dict[str, Any]) -> str:
        """flat 엔트리 → 재생 가능한 오디오 스트림 URL(재생 직전 해석). url/id 키 방어적 사용."""
        ref = entry.get("url") or entry.get("id")
        with yt_dlp.YoutubeDL({"format": "bestaudio/best", "quiet": True}) as ydl:
            info = ydl.extract_info(ref, download=False)
        return str(info["url"])

    async def _music_play_coro(self, channel_id: int, user_id: int) -> str:
        if not self._music_playlist_url:
            return "⚠️ 재생목록이 설정되지 않았습니다."
        ch = self._caller_voice_channel(user_id)
        if ch is None:
            return "🔊 먼저 음성채널에 들어가 주세요."
        if self._voice is not None and self._voice.is_connected():
            return "🎵 이미 재생 중입니다."
        loop = asyncio.get_running_loop()
        try:
            flat = await loop.run_in_executor(None, self._extract_flat)
        except Exception as e:  # yt-dlp 추출 실패(네트워크·차단 등) — 접속 안 하고 안내
            log.warning("재생목록 추출 실패: %s", type(e).__name__)
            flat = []
        if not flat:
            return "⚠️ 재생목록을 불러오지 못했습니다."
        self._voice = await ch.connect(timeout=30.0, reconnect=True)
        conn = getattr(self._voice, "_connection", None)
        log.info("음성 연결됨 can_encrypt=%s", getattr(conn, "can_encrypt", None))
        await self._await_dave_ready()
        random.shuffle(flat)
        self._music_entries = flat
        self._music_index = 0
        self._music_stopping = False
        self._music_text_ch = channel_id
        await self._play_current()
        return f"▶️ Play - {len(flat)}곡"

    async def _await_dave_ready(self) -> None:
        """E2EE(DAVE) 음성채널이면 재생 전 세션 준비(can_encrypt)를 최대 ~15초 폴링한다.

        디스코드 음성채널은 기본 E2EE(DAVE 프로토콜). DAVE 세션이 준비되기 전에 voice.play() 하면
        봇이 비암호 opus 를 흘려 디스코드가 전량 폐기 → 에러 없이 무음(after 미발화). 이 대기가
        핵심 수정. 내부 속성(_connection·can_encrypt·dave_protocol_version)은 discord.py 버전 변동에
        취약하니 방어적 getattr + try/except — 속성이 없으면 대기를 스킵하고 진행(구조 깨짐 방지).
        준비를 15초 내 못 하면 막지 않고 best-effort 재생(로그로 확증 가능).
        """
        try:
            conn = getattr(self._voice, "_connection", None)
            if not getattr(conn, "dave_protocol_version", None):
                return  # 0/None = E2EE 아님 → 대기 불필요
            for _ in range(30):  # 0.5s * 30 = 15s 상한(바운드 폴링)
                if getattr(conn, "can_encrypt", False):
                    return
                await asyncio.sleep(0.5)
            log.warning("DAVE 준비 안됨 — 재생 시도(무음 가능)")
        except Exception as e:  # 내부 속성 구조 변동 — 대기 스킵하고 진행(best-effort)
            log.warning("DAVE 준비 확인 실패(대기 스킵): %s", type(e).__name__)

    async def _play_current(self) -> None:
        """현재 인덱스 곡의 스트림을 해석해 재생. 스트림 해석 실패 곡은 건너뛰고 다음으로 진행."""
        loop = asyncio.get_running_loop()
        while self._music_entries and not self._music_stopping:
            entry = self._music_entries[self._music_index]
            try:
                url = await loop.run_in_executor(None, self._extract_stream, entry)
            except Exception as e:  # 개별 곡 스트림 실패 — 그 곡만 skip 하고 다음 곡 시도
                log.warning("곡 스트림 해석 실패(skip): %s", type(e).__name__)
                self._music_index += 1
                if self._music_index >= len(self._music_entries):
                    self._reshuffle_entries()
                    self._music_index = 0
                continue
            src = discord.FFmpegPCMAudio(
                url,
                executable=self._ffmpeg,
                before_options="-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5",
                options="-vn",
            )
            conn = getattr(self._voice, "_connection", None)
            log.info(
                "voice.play 호출 can_encrypt=%s proto=%s",
                getattr(conn, "can_encrypt", None),
                getattr(conn, "dave_protocol_version", None),
            )
            self._voice.play(src, after=self._after)
            return

    def _after(self, err: Exception | None) -> None:
        """재생 완료 콜백(discord 워커 스레드) — 정지 중이 아니면 다음 곡으로. 루프에 스케줄만.

        err=None(정상 종료)도 info 로 남긴다 — 콜백 발화 여부가 무음 진단의 핵심 신호(재발방지).
        """
        if err is not None:
            log.warning("재생 콜백 오류: %s", type(err).__name__, exc_info=err)
        else:
            log.info("재생 콜백 발화(정상 종료)")
        if self._music_stopping or self._voice is None or self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._advance(), self._loop)

    def _reshuffle_entries(self) -> None:
        """한 바퀴 다 돈 뒤 재셔플. 이미 재생된 추가곡의 'queued' 마커를 먼저 지운다 —
        안 지우면 다음 바퀴 _enqueue_coro 스캔이 스테일 마커를 건너뛰어 삽입 위치가 미세하게
        어긋난다(무해하나 정확성). 무-await(원자성 불변 유지) — 재셔플 경로 전용.
        """
        for e in self._music_entries:
            e.pop("queued", None)
        random.shuffle(self._music_entries)

    async def _advance(self) -> None:
        """다음 곡으로(끝이면 재셔플·처음부터 = 무한 반복)."""
        if self._music_stopping:
            return
        self._music_index += 1
        if self._music_index >= len(self._music_entries):
            self._reshuffle_entries()
            self._music_index = 0
        await self._play_current()

    async def _music_skip_coro(self) -> str:
        if self._voice is None or not self._voice.is_connected():
            return "⏹️ 재생 중인 곡이 없습니다."
        self._voice.stop()  # → _after → _advance 로 다음 곡
        return "⏭️ 다음 곡을 재생합니다."

    async def _music_stop_coro(self) -> str:
        if self._voice is None or not self._voice.is_connected():
            return "⏹️ 재생 중인 곡이 없습니다."
        self._music_stopping = True  # _after 가 advance 하지 않게 먼저 세팅
        self._voice.stop()
        await self._voice.disconnect()
        self._voice = None
        self._music_entries = []
        self._music_index = 0
        return "⏹️ 음악 재생을 마칩니다."

    async def _ensure_nickname(self) -> None:
        """봇 서버 닉을 _BOT_NICKNAME 으로 고정(멱등). 강퇴/재초대로 풀린 닉을 on_ready 마다 회복.

        이미 같으면 skip(불필요 edit·레이트리밋 회피). Change Nickname 권한 없으면 Forbidden(=
        DiscordException 하위) → 로그+계속. 과거 Manage Messages 없어 청소가 조용히 실패한 전례가
        있어, 닉도 안 되면 개발자가 알도록 경고를 남긴다(봇 크래시·on_ready 중단 없이).
        """
        for guild in self._client.guilds:
            me = guild.me
            if me is None or me.nick == _BOT_NICKNAME:
                continue
            try:
                await me.edit(nick=_BOT_NICKNAME)
            except discord.DiscordException as e:
                log.warning(
                    "서버 닉 고정 실패 guild=%s(%s) — Change Nickname 권한 확인 필요",
                    guild.id,
                    type(e).__name__,
                )

    async def _ensure_channels(self) -> None:
        """F1: on_ready 재진입 직렬화(재접속 중복생성 경쟁 차단) 후 실제 셋업. 셋업 경로 전용."""
        async with self._setup_lock:
            await self._run_channel_setup()

    async def _run_channel_setup(self) -> None:
        """첫 길드(치이카와)에 카테고리·채널을 멱등 구성. §4.4 + 라벨/음성/정렬/기본채널정리.

        재사용 = **1차 channelID(기존 channel_map)** → 2차 _canon. 리네임 판단은 **정확 이름 비교**
        (_canon collapse 함정 회피 — `주식-모니터링`↔`주식모니터링`을 다르게 봄). 프로젝트 채널명은
        라벨 붙여쓰기(_desired_name), channel_map 값은 폴더명 원문(라우팅 불변). 라벨 없으면 폴더명.
        재기동해도 중복 생성·리네임 없음(정확 일치면 skip). 권한/실패는 로그+계속. name 로그.
        """
        guild = self._client.guilds[0] if self._client.guilds else None
        if guild is None:
            log.warning("길드 없음 — 채널 자동생성 스킵(폴백: 채널명 매칭)")
            return
        # 카테고리 먼저 확보(이모지 rename 포함) — 코어명 탐색·멱등. 채널이 다 있어도 헤더 갱신.
        cats: dict[str, Any] = {}
        for display in _CAT_ORDER:
            try:
                cats[display] = await self._ensure_category(guild, display, _CAT_ALIASES[display])
            except discord.DiscordException as e:
                log.warning("카테고리 확보 실패 %r(%s) — 계속", display, type(e).__name__)
                cats[display] = None
        # 프로젝트 채널: 표시=라벨(붙여쓰기), tag=폴더명(라우팅). #4 순서=_PROJECT_ORDER 정본.
        proj_chans = [
            (PROJECT_LABELS.get(n, n), "project", n) for n in _project_order(self._project_names)
        ]
        plan: list[tuple[str, list[tuple[str, str, str]]]] = [
            (_CAT_SIMPLE, _SPECIAL[_CAT_SIMPLE]),
            (_CAT_PROJECT, proj_chans),
            (_CAT_DATA, _SPECIAL[_CAT_DATA]),
            (_CAT_SCHED, _SPECIAL[_CAT_SCHED]),
            (_CAT_SYSTEM, _SPECIAL[_CAT_SYSTEM]),
            (_CAT_QUESTION, _SPECIAL[_CAT_QUESTION]),
        ]
        old_by_keytag = {(k, t): cid for cid, (k, t) in self._channel_map.items()}
        by_id = {c.id: c for c in guild.channels}
        by_canon = {_canon(c.name): c for c in guild.text_channels}
        new_map: dict[int, tuple[str, str]] = {}
        project_chs: list[Any] = []  # #4 내부 정렬 대상(순서대로)
        for cat_name, chans in plan:
            category = cats.get(cat_name)
            for display, kind, tag in chans:
                target = _desired_name(display)  # 디스코드 저장형(붙여쓰기 소문자)
                # 1차: 기존 맵 channelID(이름 변형에 견고) → 2차: canon(라벨/폴더 둘 다)
                ch = by_id.get(old_by_keytag.get((kind, tag), 0))
                if ch is None:
                    ch = by_canon.get(_canon(display)) or by_canon.get(_canon(tag))
                if ch is None:  # 신규 생성
                    try:
                        ch = await guild.create_text_channel(
                            target,
                            category=category,
                            topic=_DATA_TOPIC if tag == "데이터분석" else None,
                        )
                        log.info("채널 생성: 요청=%r 저장=%r id=%s", target, ch.name, ch.id)
                        by_canon[_canon(ch.name)] = ch
                    except discord.DiscordException as e:
                        log.warning("채널 생성 실패 %r(%s) — 계속", display, type(e).__name__)
                        continue
                # 매핑 먼저(리네임 실패해도 라우팅 유지) → 정확 이름 비교로 best-effort 리네임.
                new_map[ch.id] = (kind, tag)
                if kind == "project":
                    project_chs.append(ch)
                await self._rename_if_needed(ch, target)
                if tag in _READONLY_TAGS:
                    # **여기서 예외가 새면 채널 자동생성 전체가 중단**되므로(실측: 매핑이 통째로
                    # 폴백) 이 한 채널의 부가 설정 실패는 그 채널만 로그하고 넘어간다.
                    # 설정 실패는 _ensure_readonly 안에서 조치 안내와 함께 잡는다 — 이 try 는
                    # 그 앞단(오버라이트 **조회**)까지 덮는 안전망이다.
                    try:
                        await self._ensure_readonly(guild, ch)
                    except discord.DiscordException as e:
                        log.warning("읽기전용 설정 실패 %r(%s) — 계속", display, type(e).__name__)
        await self._ensure_voice(guild, new_map, cats.get(_CAT_VOICE), old_by_keytag, by_id)
        await self._reorder_projects(project_chs)  # #4 프로젝트 채널 내부 순서
        await self._order_categories(guild)  # #3 카테고리 순서
        await self._delete_default_general(guild, new_map)  # 기본 #일반 텍스트 삭제
        await self._delete_empty_default_categories(guild)  # #5 빈 기본 카테고리 삭제
        self._channel_map = new_map
        if self._channel_map_file is not None:
            with contextlib.suppress(OSError):
                save_channel_map(self._channel_map_file, new_map)

    async def _ensure_category(self, guild: Any, display: str, aliases: list[str]) -> Any:
        """코어명(이모지·공백 제거)으로 기존 카테고리 탐색 → 이모지 display 로 rename. 없으면 생성.

        #1: 카테고리 헤더에 이모지. 채널 리네임과 같은 사상 — 코어명으로 찾고 정확명 다르면 rename.
        음성 카테고리는 이전 이름 '음성'도 별칭이라 '음성'→'🎵 PlayList' 로 이관된다.
        """
        cores = {_cat_core(a) for a in aliases}
        cat = next((c for c in guild.categories if _cat_core(c.name) in cores), None)
        if cat is None:
            return await guild.create_category(display)
        if cat.name != display:  # 정확명 비교 — 이미 이모지형이면 skip(멱등)
            log.info("카테고리 리네임: %r → %r", cat.name, display)
            await cat.edit(name=display)
        return cat

    async def _ensure_readonly(self, guild: Any, ch: Any) -> None:
        """사람은 못 쓰고 **봇만 쓰는** 채널로 만든다(@everyone 쓰기 금지 · 봇 쓰기 허용).

        `#미국주식` 은 매일 카드를 읽기만 하는 채널이라 메시지 왕복이 없다(사용자 요청) —
        사람이 쓴 글이 섞이면 카드 흐름이 끊긴다.

        **먼저 읽고, 필요할 때만 쓴다.** 오버라이트 조회는 '역할 관리' 권한 없이도 되므로,
        이미 읽기 전용이면(사람이 채널 설정에서 손으로 해둔 경우 포함) **아무것도 하지 않고
        로그도 남기지 않는다.**

        ⚠️ **부분 적용이 가장 위험하다** — @everyone 거부는 걸렸는데 봇 허용이 빠지면 **봇이 잠겨
        카드가 안 나간다**. 그래서 ① 둘 다 갖춰야 "완료"로 보고 ② 고칠 때는 **봇 허용을 먼저** 건다.

        🔴 **`set_permissions` 는 부분 갱신이 아니라 전체 치환이다**(discord.py 2.7.1 — kwargs 로
        `PermissionOverwrite` 를 새로 만들어 PUT). `send_messages=` 만 넘기면 그 대상의 **다른
        권한이 전부 지워진다**(이 채널은 @everyone·봇 모두 `채널 보기` 오버라이트를 갖고 있다).
        → **읽은 오버라이트 객체를 고쳐 `overwrite=` 로 되돌려준다**(read-modify-write).
        `overwrites_for()` 는 매번 새 객체라 추가 조회가 없다.
        ⚠️ **`@everyone` 의 `view_channel` 은 건드리지 않는다** — 공개/비공개는 사용자 정책이다.
        """
        everyone, me = getattr(guild, "default_role", None), getattr(guild, "me", None)
        if everyone is None or me is None:
            log.warning("길드 역할 정보 없음 — 읽기전용 설정 스킵 %r", ch.name)
            return
        ow_all, ow_me = ch.overwrites_for(everyone), ch.overwrites_for(me)
        blocked = ow_all.send_messages is False  # 사람 쓰기 막혔나
        # 봇이 채널을 **실제로 볼 수 있는가** — 못 보면 쓰기 권한이 있어도 카드가 안 나간다.
        # 손으로 계산하지 않는다: 유효 권한은 길드 역할 권한 → 채널 @everyone → 채널 역할 →
        # 채널 멤버 오버라이트(Administrator 는 단축) 순으로 접히는데, 오버라이트 두 개만 보면
        # 어긋난다. discord.py 가 캐시로 계산해 준다(추가 호출 0).
        bot_can_view = ch.permissions_for(me).view_channel
        # ⚠️ 비대칭이 의도다 — view 는 **효력**으로 보고 send 는 **명시 허용(is True)** 만 인정한다.
        # 우리가 곧 @everyone 의 send 를 막을 것이라, 상속을 완료로 읽으면 그 순간 봇이 잠긴다.
        bot_ok = ow_me.send_messages is True and bot_can_view
        if blocked and bot_ok:
            return  # 이미 읽기 전용 — 조용히 지나간다
        try:
            if not bot_ok:  # **봇 먼저** — 여기서 실패해도 사람 권한은 그대로 남는다
                ow_me.send_messages = True
                if not bot_can_view:  # 볼 수 없을 때만 — 같은 객체라 호출은 여전히 1회
                    ow_me.view_channel = True
                await ch.set_permissions(me, overwrite=ow_me)
                bot_ok = True  # 아래 경고가 **낡은 값**을 말하지 않게 즉시 반영
            if not blocked:
                ow_all.send_messages = False  # view_channel 은 그대로 둔다(사용자 정책)
                await ch.set_permissions(everyone, overwrite=ow_all)
                blocked = True
        except discord.DiscordException as e:
            # 경고를 없애지 않는다 — 채널이 실제로 읽기 전용이 아닌데 조용하면 아무도 모른다.
            # 대신 **무엇을 하면 되는지**를 적는다(봇에 '역할 관리'가 없으면 코드로는 못 고친다).
            log.warning(
                "#%s 읽기전용 설정 실패(%s) — 채널 설정에서 @everyone '메시지 보내기' ✗ + "
                "봇 ✓ 로 직접 지정하거나 봇 역할에 '역할 관리'를 주세요 "
                "(현재 @everyone 차단=%s · 봇 허용=%s)",
                ch.name,
                type(e).__name__,
                blocked,
                bot_ok,
            )
            return
        log.info("채널 읽기전용 설정: %r id=%s", ch.name, ch.id)

    async def _rename_if_needed(self, ch: Any, target: str) -> None:
        """정확 이름 비교 best-effort 리네임(같으면 skip). 실패는 로그+기존명 보존(매핑 유지)."""
        if ch.name == target:
            return
        old = ch.name
        try:
            await ch.edit(name=target)
            log.info("채널 리네임: %r → %r 저장=%r id=%s", old, target, ch.name, ch.id)
        except discord.DiscordException as e:
            status = getattr(e, "status", "?")  # HTTPException 이면 HTTP status(400/429 진단)
            log.warning("리네임 실패 %r(%s status=%s) — 기존명 보존", old, type(e).__name__, status)

    async def _ensure_voice(
        self,
        guild: Any,
        new_map: dict[int, tuple[str, str]],
        category: Any,
        old_by_keytag: dict[tuple[str, str], int],
        by_id: dict[int, Any],
    ) -> None:
        """음성 PlayList 멱등 확보 — 맵 channelID 1차·이름/기본음성 2차·없으면 생성. 이름=PlayList.

        category = 미리 확보된 🎵 PlayList 카테고리. channel_map 에 ("role","playlist") 기록(텍스트
        라우팅 무관·관리용). 매핑 먼저(리네임 실패해도 유지). 실패는 로그+계속.
        """
        try:
            ch = by_id.get(old_by_keytag.get(("role", "playlist"), 0))
            if ch is None:  # PlayList 이름 매칭 → 기본 음성 '일반'
                ch = next(
                    (c for c in guild.voice_channels if _canon(c.name) == _canon(_VOICE_NAME)),
                    None,
                ) or next((c for c in guild.voice_channels if c.name in _DEFAULT_GENERAL), None)
            if ch is None:  # 없으면 생성
                ch = await guild.create_voice_channel(_VOICE_NAME, category=category)
                log.info("음성 생성: 저장=%r id=%s", ch.name, ch.id)
        except discord.DiscordException as e:
            log.warning("PlayList 음성 처리 실패(%s) — 계속", type(e).__name__)
            return
        new_map[ch.id] = ("role", "playlist")  # 매핑 먼저(리네임 실패해도 유지)
        if ch.name != _VOICE_NAME:  # 정확명 비교 멱등(이미 PlayList 면 skip)
            old = ch.name
            try:
                await ch.edit(name=_VOICE_NAME, category=category)
                log.info("음성 리네임: %r → %r 저장=%r id=%s", old, _VOICE_NAME, ch.name, ch.id)
            except discord.DiscordException as e:
                st = getattr(e, "status", "?")
                log.warning("음성 리네임 실패 %r(status=%s) — 기존명 보존", old, st)

    async def _reorder_projects(self, project_chs: list[Any]) -> None:
        """#4 프로젝트 카테고리 내부 순서 = _PROJECT_ORDER 정본(project_chs 가 이미 그 순서).

        현재 position 정렬 순서가 목표와 같으면 skip(멱등). 다르면 순서대로 edit(position=i).
        ponytail: position 은 길드 전역 상대값이라 절대치는 라이브에서 조정될 수 있다 — 순서만 보장
        (같은 카테고리 내 상대순). 반복 기동 시 이미 정렬돼 skip.
        """
        if not project_chs:
            return
        current = sorted(project_chs, key=lambda c: c.position)
        if [c.id for c in current] == [c.id for c in project_chs]:
            return  # 이미 순서 맞음
        for i, ch in enumerate(project_chs):
            try:
                await ch.edit(position=i)
            except discord.DiscordException as e:
                log.warning("프로젝트 채널 정렬 실패 %r(%s) — 계속", ch.name, type(e).__name__)

    async def _order_categories(self, guild: Any) -> None:
        """봇 카테고리를 _CAT_ORDER 순서로 재정렬(멱등·코어명 매칭). 실패는 로그+계속."""
        for pos, display in enumerate(_CAT_ORDER):
            cores = {_cat_core(a) for a in _CAT_ALIASES[display]}
            cat = next((c for c in guild.categories if _cat_core(c.name) in cores), None)
            if cat is not None and cat.position != pos:
                try:
                    await cat.edit(position=pos)
                except discord.DiscordException as e:
                    log.warning("카테고리 정렬 실패 %r(%s) — 계속", display, type(e).__name__)

    async def _delete_empty_default_categories(self, guild: Any) -> None:
        """#5 디스코드 기본 빈 카테고리 삭제 — 이름 매칭 AND 자식 0 이중 가드(봇 카테고리 제외)."""
        for cat in list(guild.categories):
            if cat.name in _DEFAULT_CATEGORIES and not cat.channels:
                try:
                    await cat.delete()
                    log.info("빈 기본 카테고리 삭제: %r", cat.name)
                except discord.DiscordException as e:
                    log.warning("카테고리 삭제 실패 %r(%s) — 보존", cat.name, type(e).__name__)

    async def _delete_default_general(
        self, guild: Any, new_map: dict[int, tuple[str, str]]
    ) -> None:
        """기본 텍스트 채널 #일반(general) 삭제 — 정확 매칭 + 봇 생성분(new_map) 제외 가드."""
        general = next(
            (c for c in guild.text_channels if c.name in _DEFAULT_GENERAL and c.id not in new_map),
            None,
        )
        if general is None:
            return
        try:
            await general.delete()
            log.info("기본 텍스트 채널 삭제: %r id=%s", general.name, general.id)
        except discord.DiscordException as e:
            log.warning("기본 채널 삭제 실패(%s) — 계속", type(e).__name__)

    # ── 이벤트 등록(전용 스레드 이벤트루프에서 발화) ──────────────────────────
    def _register_events(self) -> None:
        @self._client.event
        async def on_ready() -> None:
            # 접속 후 닉 고정 → 채널 자동생성(각 1회). 서로·부팅 안 막게 예외 격리(_ready 항상 set).
            try:
                await self._ensure_nickname()
            except Exception as e:  # 닉 실패가 채널 셋업·부팅을 막지 않게 격리(로그+계속)
                log.warning("서버 닉 고정 실패(%s) — 계속", type(e).__name__)
            try:
                await self._ensure_channels()
            except Exception as e:  # 권한 없음·API 오류 등 — 로그+계속(브리지 안 죽게)
                log.error("채널 자동생성 실패(%s) — 폴백: 채널명 매칭", type(e).__name__)
            finally:
                self._ready.set()  # 접속 완료 — wait_ready 대기자 해제

        @self._client.event
        async def on_message(message: discord.Message) -> None:
            await self._on_message(message)

        @self._client.event
        async def on_interaction(interaction: discord.Interaction) -> None:
            await self._on_interaction(interaction)

    def _is_playlist_channel(self, channel_id: int) -> bool:
        """channel_map 상 ("role","playlist") 채널인지 — 인가 우회 선필터용(코어가 최종 판정)."""
        return self._channel_map.get(channel_id) == ("role", "playlist")

    def _is_guest_channel(self, channel_id: int) -> bool:
        """channel_map 상 ("role","게스트질문") 채널인지 — 인가 우회 선필터용(코어가 최종 판정)."""
        return self._channel_map.get(channel_id) == ("role", "게스트질문")

    async def _on_message(self, message: discord.Message) -> None:
        """텍스트/사진 메시지 → 정규화 Event 큐 적재. 자기 메시지·비허용은 드롭."""
        me = self._client.user
        if me is not None and message.author.id == me.id:
            return  # 자기 메시지 무시(에코 루프 방지)
        # 선-필터(코어도 재검증). 단 플레이리스트·게스트질문 채널은 비인가라도 통과 — 코어가
        # 화이트리스트 음악 명령(_playlist_bypass)·순수 질문 텍스트(_guest_bypass)만 우회 허용한다.
        # 그 외 채널의 비인가는 여기서 드롭(스팸 유입 차단). 게스트 버튼/사진은 코어가 최종 배제.
        cid = message.channel.id
        bypass = self._is_playlist_channel(cid) or self._is_guest_channel(cid)
        if message.author.id not in self._allowed and not bypass:
            return
        self._queue.put(self._message_event(message))

    def _message_event(self, message: discord.Message) -> Event:
        """discord.Message → Event(§1.4). 이미지 첨부가 있으면 photo, 아니면 text."""
        channel = message.channel
        # 채널→라우팅: channel_map(channelID) 우선 — 프로젝트 채널=폴더명 원문, 특수 채널=역할.
        # 매핑 없으면(자동생성 실패·미매핑) 폴백=채널명을 프로젝트 후보로(DM 은 name 없음 → None).
        entry = self._channel_map.get(channel.id)
        if entry is not None:
            kind, tag = entry
            project = tag if kind == "project" else None
            channel_role = tag if kind == "role" else None
        else:
            project = getattr(channel, "name", None)
            channel_role = None
        # §4.7 델타2: 답장 이어가기(④). message.reference.message_id 를 채운다(소비는 1c).
        ref = message.reference
        reply_to = ref.message_id if ref is not None else None
        image = next(
            (a for a in message.attachments if Path(a.filename or "").suffix.lower() in PHOTO_EXTS),
            None,
        )
        if image is not None:
            return Event(
                kind="photo",
                channel_id=channel.id,
                user_id=message.author.id,
                text=message.content or "",
                message_id=message.id,
                photo_ref=image.url,
                project=project,
                reply_to=reply_to,
                channel_role=channel_role,
            )
        return Event(
            kind="text",
            channel_id=channel.id,
            user_id=message.author.id,
            text=message.content or "",
            message_id=message.id,
            project=project,
            reply_to=reply_to,
            channel_role=channel_role,
        )

    async def _on_interaction(self, interaction: discord.Interaction) -> None:
        """버튼(component) 탭 → 즉시 defer(3초 규약, §2.3) 후 정규화 Event 큐 적재.

        비허용 유저는 defer·적재 없이 드롭(불필요 API·맵 누증 차단 — 코어도 재검증). defer 를
        **큐 적재 전** 이벤트루프 스레드에서 먼저 호출해, 워커 지연과 무관하게 3초 규약 유지.
        """
        if interaction.type != discord.InteractionType.component:
            return  # 컴포넌트 외(슬래시 명령 등)는 0단계 미사용
        ch_id = interaction.channel_id or 0
        # 선-필터: 비인가는 드롭(defer 도 안 함). 단 플레이리스트 채널 버튼은 통과 — 코어가
        # clean:ok/x(ㅁ청소 확인·취소)만 우회 허용(_playlist_bypass), 그 외는 코어 게이트가 무시.
        if interaction.user.id not in self._allowed and not self._is_playlist_channel(ch_id):
            return
        try:
            await interaction.response.defer()  # §2.3 3초 규약 — 큐 적재보다 반드시 선행
        except discord.HTTPException as e:
            log.warning("interaction defer 실패: %s", type(e).__name__)
        data: dict[str, Any] = interaction.data if isinstance(interaction.data, dict) else {}
        custom_id = data.get("custom_id")
        parsed = parse_callback(custom_id) if isinstance(custom_id, str) else None
        action, arg = parsed if parsed is not None else ("", "")
        callback_id = str(interaction.id)
        self._interactions[callback_id] = interaction  # ack(followup)로 잇기
        msg = interaction.message
        entry = self._channel_map.get(ch_id)  # 코어 _playlist_bypass 가 channel_role 로 우회 판정
        self._queue.put(
            Event(
                kind="button",
                channel_id=ch_id,
                user_id=interaction.user.id,
                message_id=msg.id if msg is not None else None,
                action=action,
                action_arg=arg,
                callback_id=callback_id,
                channel_role=entry[1] if entry is not None and entry[0] == "role" else None,
            )
        )

    # ── 봇 스레드(전용 이벤트루프) ─────────────────────────────────────────────
    def _run_bot(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._client.start(self.token))
        except (asyncio.CancelledError, RuntimeError):
            pass
        except discord.DiscordException as e:
            # L-1: 로그인 실패(잘못된 토큰=LoginFailure)·게이트웨이 예외가 미포착이면 봇 스레드가
            # 조용히 죽고 poll 이 queue.get() 에서 영구 블록된다. 명시 로그(토큰 미노출) 후 finally
            # 의 종료 센티넬로 poll·main 을 깨워 깨끗이 종료시킨다.
            log.error("디스코드 봇 스레드 종료(%s) — 토큰·권한을 확인하세요", type(e).__name__)
            self.bot_failed = True  # main 이 종료코드 1 로 올린다(정상 종료와 구분).
        finally:
            with contextlib.suppress(RuntimeError, discord.DiscordException):
                loop.run_until_complete(self._client.close())
            loop.close()
            # L-1: 봇 스레드가 어떤 이유로든 끝나면 poll 해제(센티넬) — 봇 사망 시 무한 블록 방지.
            self._closed = True
            self._queue.put(None)
            log.info("디스코드 이벤트루프 종료")

    def _start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run_bot, name="discord-bot", daemon=True)
            self._thread.start()

    # ── 수신: 큐 직렬 소비 제너레이터(§2.5) ────────────────────────────────────
    def poll(self) -> Iterator[Event]:
        """봇 스레드 기동 후 큐를 직렬 소비하는 블로킹 제너레이터. close() 시 센티넬로 종료."""
        self._start()
        while not self._closed:
            item = self._queue.get()
            if item is None:  # close() 가 넣은 종료 센티넬
                break
            yield item

    # ── 송신(§2.1) ────────────────────────────────────────────────────────────
    def _render_parts(self, text: str) -> list[Any]:
        """마스킹 후 발송 파트 리스트(§4.1). 상태 헤더 매칭 → [Embed, 오버플로 plain 청크…],
        아니면 기존대로 plain 청크. 파트는 str(plain 콘텐츠) 또는 discord.Embed.
        """
        masked = mask_secrets(text, self.secrets)
        color = _status_color(masked)
        if color is None:
            return [chunk or "(빈 응답)" for chunk in chunk_text(masked, self.limit)]
        embed, overflow = _build_embed(masked, color)
        parts: list[Any] = [embed]
        if overflow:
            parts += [chunk or "(빈 응답)" for chunk in chunk_text(overflow, self.limit)]
        return parts

    def _emit(
        self,
        text: str,
        buttons: list[Button] | None,
        coro: Callable[[Any, Any], Coroutine[Any, Any, Any]],
    ) -> int | None:
        """마스킹·상태판정·청킹·버튼(마지막 파트만) 공통 발송 루프. coro(part, view)→id. 첫 id 반환.

        send 가 대상 해석 코루틴을 주입해 발송 규칙(§2.1)을 단일 소스로 공유한다.
        """
        parts = self._render_parts(text)
        last = len(parts) - 1
        first_id: int | None = None
        for i, part in enumerate(parts):
            view = render_view(buttons) if buttons is not None and i == last else None
            mid = self._run(coro(part, view))
            if i == 0:
                first_id = mid if isinstance(mid, int) else None
        return first_id

    def send(
        self,
        channel_id: int,
        text: str,
        buttons: list[Button] | None = None,
        card: dict[str, Any] | None = None,
    ) -> int | None:
        """마스킹 후 청크 분할 전송. 버튼은 마지막 청크에만. 첫 청크 message_id 반환(실패 None).

        예외 1: `card` 가 있으면 구조화 Embed 1장으로 렌더한다(다이제스트 전용 경로 —
        `text` 는 카드를 못 그리는 어댑터용 폴백이라 여기선 쓰지 않는다). 카드는 항상 단일
        메시지라 청킹·오버플로가 없다(초과분은 build_card_embed 가 슬롯별로 자름).
        예외 2: 세로 목록(전부 p:/c: 액션)은 세로 1열 V2 LayoutView 로 렌더(헤더 텍스트도 흡수).
        V2 flag 는 메시지 생성 시 고정(edit 로 classic 전환 불가)이라, 그 id 를 _v2_messages 에
        기록해 이후 edit 도 V2 로 유지한다(선택지 갱신·버튼 제거 편집이 400 나지 않게).
        """
        if card is not None:
            embed = build_card_embed(card, self.secrets)
            view = render_view(buttons) if buttons else None
            mid = self._run(self._send_coro(channel_id, embed, view))
            return mid if isinstance(mid, int) else None
        if _is_vertical_list(buttons):
            assert buttons is not None  # _is_vertical_list 가 보장(mypy 좁히기)
            view = render_project_view(mask_secrets(text, self.secrets), buttons)
            mid = self._run(self._send_view_coro(channel_id, view))
            mid = mid if isinstance(mid, int) else None
            if mid is not None:
                self._v2_messages.add(mid)
            return mid
        return self._emit(text, buttons, lambda body, view: self._send_coro(channel_id, body, view))

    def edit(
        self,
        channel_id: int,
        message_id: int,
        text: str,
        buttons: list[Button] | None = None,
        card: dict[str, Any] | None = None,
    ) -> None:
        """진행 메시지 in-place 갱신. 오버플로(§2.2): 첫 파트 편집 + 나머지 후속 발행, 버튼 말미.

        상태 헤더면 첫 파트가 Embed 라 진행(노랑)→완료(초록)/실패(빨강) 전이가 같은 message_id
        편집으로 색만 바뀐다(§4.0 상태 전이=같은 메시지 편집).

        예외 1: `card` 가 있으면 구조화 Embed 로 교체(다이제스트 버튼 반영 — footer·버튼만 바뀐다).
        예외 2: send 가 V2(세로 목록)로 만든 메시지는 flag 가 고정이라 classic 으로 못 돌린다 →
        같은 세로 V2 로 편집한다(버튼 갱신·제거·만료 문구 모두 TextDisplay 흡수). buttons 없으면
        빈 목록으로 렌더해 버튼만 사라진다(텍스트는 유지).
        """
        if message_id in self._v2_messages:
            view = render_project_view(mask_secrets(text, self.secrets), buttons or [])
            self._run(self._edit_view_coro(channel_id, message_id, view))
            return
        if card is not None:
            embed = build_card_embed(card, self.secrets)
            head_view = render_view(buttons) if buttons else None
            self._run(self._edit_coro(channel_id, message_id, embed, head_view))
            return
        parts = self._render_parts(text)
        last = len(parts) - 1
        head_view = render_view(buttons) if buttons is not None and last == 0 else None
        self._run(self._edit_coro(channel_id, message_id, parts[0], head_view))
        for j, extra in enumerate(parts[1:], start=1):
            view = render_view(buttons) if buttons is not None and j == last else None
            self._run(self._send_coro(channel_id, extra, view))

    def ack(self, callback_id: str | None, note: str | None = None) -> None:
        """이미 defer 됨(§2.3) → note 있으면 followup.send, 없으면 no-op. callback_id 소비(정리)."""
        if not callback_id:
            return
        interaction = self._interactions.pop(callback_id, None)
        if interaction is None:
            return  # 이미 소비/미등록 — no-op(멱등)
        if note:
            self._run(self._followup_coro(interaction, note))

    def fetch_file(self, photo_ref: str, dest_dir: Path) -> Path:
        """attachment.url 다운로드 — 디스코드 CDN 도메인·확장자·크기·트래버설 잠금(§2.4 계승).

        저장명은 URL 경로의 basename 만(쿼리·경로 성분 제거 → 트래버설 차단). 위반은 ValueError.
        """
        parsed = urllib.parse.urlparse(photo_ref)
        if parsed.scheme != "https" or parsed.hostname not in _DISCORD_CDN_HOSTS:
            raise ValueError(f"허용되지 않은 다운로드 도메인: {parsed.hostname!r}")
        name = Path(urllib.parse.unquote(parsed.path)).name  # basename 만 — 경로/쿼리 제거
        if not name or name in (".", ".."):
            raise ValueError("잘못된 파일명")
        ext = Path(name).suffix.lower()
        if ext not in PHOTO_EXTS:
            raise ValueError(f"허용되지 않은 확장자: {ext!r}")
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / name
        # ⚠️ 함정 재발 방지: 디스코드 CDN(Cloudflare)은 urllib 기본 UA(Python-urllib/*)를 403 차단
        # (작업일지 함정 "REST 직접 호출 시 User-Agent 필수"와 동일 — 라이브 실측 403→200 확인).
        req = urllib.request.Request(  # https + CDN 화이트리스트 통과분만(SSRF 차단)
            photo_ref, headers={"User-Agent": "Mozilla/5.0"}
        )
        # M-3: 리다이렉트 차단 opener — CDN 이 3xx 로 내부주소를 가리켜도 추종 안 함(SSRF 차단).
        with _NOREDIRECT_OPENER.open(req, timeout=30) as resp:  # 스킴·호스트 검증됨
            clen = resp.headers.get("Content-Length")
            if clen is not None and clen.isdigit() and int(clen) > MAX_PHOTO_BYTES:
                raise ValueError("사진이 크기 상한(10MB)을 초과합니다.")
            payload = resp.read(MAX_PHOTO_BYTES + 1)  # 상한+1 만 읽어 초과 즉시 판정(메모리 보호)
        if len(payload) > MAX_PHOTO_BYTES:
            raise ValueError("사진이 크기 상한(10MB)을 초과합니다.")
        dest.write_bytes(payload)
        return dest

    def close(self) -> None:
        """Gateway·이벤트루프·워커 정리. 중복 호출 무해."""
        self._closed = True
        self._queue.put(None)  # poll() 제너레이터 해제(센티넬)
        loop = self._loop
        if loop is not None and loop.is_running():
            # 다른 스레드에서 안전하게 종료 요청(봇 루프가 client.close() 수행).
            asyncio.run_coroutine_threadsafe(self._client.close(), loop)
        if self._thread is not None:
            self._thread.join(timeout=5)

    # ── asyncio↔동기 경계 헬퍼(§3.2) ──────────────────────────────────────────
    def _run(self, coro: Coroutine[Any, Any, Any], timeout: float = _CALL_TIMEOUT) -> Any:
        """코루틴을 봇 이벤트루프에 밀어넣고 완료까지 동기 대기. 실패는 로그+None(§3.3, 루프 보호).

        플랫폼 오류(rate-limit·네트워크·루프 사망)는 어댑터가 삼키고 로그만 남긴다(코어 직렬 루프
        보호). 그래서 광범위 except — send/edit/ack 계약이 "실패는 로그·None"이기 때문(§3.3).

        timeout 은 per-call — 기본은 REST 용 _CALL_TIMEOUT(30) 이나, 음성 경로(connect+DAVE 대기+
        추출)는 30초를 넘을 수 있어 play/stop/skip 은 넉넉히 넘긴다(§ 음악 재생).
        """
        loop = self._loop
        if loop is None or not loop.is_running():
            log.warning("디스코드 이벤트루프 미준비 — 호출 스킵")
            coro.close()
            return None
        try:
            fut = asyncio.run_coroutine_threadsafe(coro, loop)
            return fut.result(timeout=timeout)
        except Exception as e:  # §3.3: 모든 플랫폼 오류를 삼키고 로그(코어 직렬 루프 보호)
            # 사유까지 남긴다(2026-08-13) — 타입만으로는 왜 400 인지 알 수 없어, 실제로
            # 카드가 안 뜨는 원인을 찾는 데 재기동을 몇 번 반복해야 했다. HTTPException 의
            # 본문은 디스코드 API 의 검증 메시지(어느 필드가 왜 거부됐는지)라 진단 가치가 크다.
            # 300자로 자른다 — 컴포넌트 오류는 항목별로 길게 나온다.
            # `except Exception` 이라 디스코드 HTTPException 만 오는 게 아니다 — aiohttp 는 URL·
            # 호스트를, 우리 코루틴의 ValueError/TypeError 는 페이로드 조각을 문자열에 싣는다.
            # 이 모듈은 머리말에서 «전송 직전 mask_secrets 로 방어심층 마스킹» 을 선언했으므로
            # 로그도 같은 문을 지난다(`.env` 값·내부 절대경로가 대상, 2026-08-14).
            log.warning(
                "디스코드 호출 실패: %s — %s",
                type(e).__name__,
                mask_secrets(str(e), self.secrets)[:300],
            )
            return None

    async def _send_coro(self, channel_id: int, payload: Any, view: Any) -> int | None:
        channel = self._client.get_channel(channel_id) or await self._client.fetch_channel(
            channel_id
        )
        msg = await channel.send(**_send_kwargs(payload, view))  # payload=plain str 또는 Embed
        return int(msg.id)

    async def _send_view_coro(self, channel_id: int, view: Any) -> int | None:
        """V2 LayoutView 전용 발송(content 없음 — 헤더는 view 의 TextDisplay 에 흡수됨)."""
        channel = self._client.get_channel(channel_id) or await self._client.fetch_channel(
            channel_id
        )
        msg = await channel.send(view=view)
        return int(msg.id)

    async def _edit_view_coro(self, channel_id: int, message_id: int, view: Any) -> None:
        """V2 LayoutView 전용 편집(content/embed 없음 — V2 는 content 를 못 실어 view 만 교체)."""
        channel = self._client.get_channel(channel_id) or await self._client.fetch_channel(
            channel_id
        )
        await channel.get_partial_message(message_id).edit(view=view)

    async def _edit_coro(self, channel_id: int, message_id: int, payload: Any, view: Any) -> None:
        channel = self._client.get_channel(channel_id) or await self._client.fetch_channel(
            channel_id
        )
        # 반대 필드를 명시적 None 으로 지워 plain↔Embed 전이가 잔여물 없이 치환되게 한다
        # (content=None → 텍스트 제거, embed=None → 임베드 제거, view=None → 컴포넌트 제거).
        if isinstance(payload, discord.Embed):
            content, embed = None, payload
        else:
            content, embed = payload, None
        await channel.get_partial_message(message_id).edit(content=content, embed=embed, view=view)

    async def _followup_coro(self, interaction: Any, note: str) -> None:
        await interaction.followup.send(note)

    async def _purge_coro(self, channel_id: int) -> int:
        """채널 메시지를 100개씩 반복 purge 해 전부 삭제, 삭제 건수 합 반환(§clear_channel).

        bulk=True 는 14일 이내는 일괄삭제, 14일 초과분은 개별삭제로 폴백한다(discord.py 규약).
        100개 미만이 돌아오면 히스토리 소진으로 보고 종료. Manage Messages 권한 없음·API 오류는
        DiscordException 을 삼켜 그때까지 삭제한 건수만 반환(부분 성공 허용) 후 로깅한다.
        """
        channel = self._client.get_channel(channel_id) or await self._client.fetch_channel(
            channel_id
        )
        total = 0
        try:
            while True:
                deleted = await channel.purge(limit=100, bulk=True)
                total += len(deleted)
                if len(deleted) < 100:  # 이번 배치가 100 미만 = 더 지울 게 없음
                    break
        except discord.DiscordException as e:
            log.warning("채널 청소 부분 실패(%s) — %d개 삭제 후 중단", type(e).__name__, total)
        return total
