#!/usr/bin/env python3
"""adapter.py — 플랫폼 무관 어댑터 계약(동결: docs/기능/디스코드_이관/02_계약.md §1·§2).

코어는 이 2개 dataclass(`Event`·`Button`)와 `Adapter` 계약 메서드만 안다. 플랫폼(현재 구현:
디스코드) 이벤트는 어댑터가 `Event` 로 정규화하고, 코어의 `Button` 리스트를 플랫폼 UI 로 렌더한다.
플랫폼 라이브러리(discord.py)는 어댑터 파일(discord_adapter.py)에만 격리되고, 이 모듈과 코어는
표준 라이브러리만 쓴다. 콜백 코덱·청킹·다운로드 가드 등 플랫폼 무관 공유 유틸도 여기 둔다
(어댑터가 바뀌어도 재사용 — 플랫폼 교체 seam).
"""

from __future__ import annotations

import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class Button:
    """추상 버튼 스펙. 어댑터가 플랫폼 UI(현재 구현: discord.ui.Button)로 렌더한다."""

    label: str
    # 정규화 액션: push|x|p|c|nb:*(_NB_VERBS)|od:rev + §4.7 델타3(r|rec|fav|…).
    action: str
    arg: str = ""  # 액션 인자(프로젝트명·item_id·"mid:idx"·idx 등)
    # 어댑터가 플랫폼 색으로 매핑(§4.7 델타1): success=승인(초록)/primary=실행(블루)/
    # danger=파괴 전용(빨강)/secondary=그 외(회색). "default"는 secondary 동의어(하위호환).
    style: str = "default"  # "default"|"secondary"|"primary"|"success"|"danger"


@dataclass(frozen=True)
class Event:
    """플랫폼 이벤트를 정규화한 코어 입력."""

    kind: str  # "text"|"photo"|"button"|"command"
    channel_id: int  # 격리·라우팅 키 (channel.id)
    user_id: int  # 인가 키 (author.id) — 허용목록 대조
    text: str = ""  # 본문 or 사진 캡션
    message_id: int | None = None  # 편집 대상
    action: str = ""  # 버튼 정규화 액션(kind=="button")
    action_arg: str = ""  # 버튼 인자
    callback_id: str | None = None  # ack 핸들 (interaction 토큰)
    photo_ref: str | None = None  # 파일 핸들 (attachment.url)
    project: str | None = None  # 어댑터가 채널→프로젝트(channel_map)를 미리 채움. 미매핑(DM)은 None
    # §4.7 델타2: 답장 이어가기(④). message.reference.msg_id.
    # 1a 는 어댑터가 채우기만·코어 소비는 1c(frozen 기본값이라 기존 생성부는 무영향).
    reply_to: int | None = None
    # ①(채널 자동생성): 특수 채널 역할 태그("간단처리"|"데이터분석"|"알림"|"봇상태"). 어댑터가
    # channel_map 으로 채운다. 프로젝트 채널은 project, 특수 채널은 이 필드로 라우팅. 미매핑은 None.
    channel_role: str | None = None


# ── Card 규약(send/edit 의 선택 인자 `card`) ────────────────────────────────
# 평범한 dict 다(dataclass·TypedDict 아님 — 코어가 만들고 어댑터가 읽기만 하는 렌더 힌트).
#   {"author": str, "title": str, "description": str,
#    "fields": [(name, value, inline)], "footer": str, "color": int}
# 전부 선택 — 없는 키는 그 슬롯을 비운다. 플랫폼 한도 절단은 **어댑터 책임**(디스코드 기준
# author/title 256 · description 4096 · field value 1024 · footer 2048). 코어는 의미만 만든다.
# 현재 유일한 사용처 = 🧩 오픈소스 다이제스트 카드(디스코드 Embed fields 렌더, 2026-07-27).
class Adapter(Protocol):
    """플랫폼 어댑터 계약(동결). 코어는 이 인터페이스만 호출한다.

    계약 메서드(poll·send·edit·ack·fetch_file·close·setup_channels·role_channel·project_channel·
    clear_channel·play_music·stop_music·skip_music)와 2 dataclass 는 플랫폼 교체 seam 이라 불변
    (과설계 금지, §5.1). play/stop/skip_music 은 음성재생 capability(디스코드 전용). 현재 구현은
    디스코드 1개지만, 계약을 이 인터페이스로 고정해 다른 플랫폼으로 교체 가능한 구조를 유지한다.
    `secrets`: 마스킹 대상(봇토큰·내부경로). 생성 시 주입·보관(§2.1) — 코어의 L-1 진행 마스킹이
    잘라내기 전에 참조하므로 계약면에 노출한다(공유 속성 1개).
    """

    secrets: list[str]

    def poll(self) -> Iterator[Event]:
        """이벤트 소스(블로킹 제너레이터). 네트워크 오류는 내부 로그·재시도, close() 후 종료."""
        ...

    def send(
        self,
        channel_id: int,
        text: str,
        buttons: list[Button] | None = None,
        card: dict[str, Any] | None = None,
    ) -> int | None:
        """전송(마스킹·청킹·버튼렌더는 어댑터 흡수). 첫 청크 message_id | 실패 시 None.

        `card`(선택, 2026-07-27 추가): 구조화 카드 스펙 — 아래 Card 규약. 리치 렌더가 되는
        어댑터는 이걸로 그리고, 못 그리는 어댑터는 **무시하고 `text` 를 그대로 보낸다**
        (텍스트가 항상 폴백이라 정보 손실 0). 기존 호출은 인자를 안 주면 종전과 동일.
        """
        ...

    def edit(
        self,
        channel_id: int,
        message_id: int,
        text: str,
        buttons: list[Button] | None = None,
        card: dict[str, Any] | None = None,
    ) -> None:
        """진행 메시지 in-place 갱신(+오버플로 후속 발행). 실패는 로그만·계속.

        `card`: send 와 같은 규약(미지원 어댑터는 무시하고 `text` 로 편집).
        """
        ...

    def ack(self, callback_id: str | None, note: str | None = None) -> None:
        """버튼 탭 응답(디스코드 followup). callback_id=None 이면 no-op."""
        ...

    def fetch_file(self, photo_ref: str, dest_dir: Path) -> Path:
        """파일 다운로드(도메인·크기·확장자·트래버설 잠금). 위반·실패는 예외 전파."""
        ...

    def close(self) -> None:
        """연결 정리(디스코드 Gateway 종료). 중복 호출 무해."""
        ...

    def setup_channels(self, project_names: list[str]) -> None:
        """①(채널 자동생성): 봇 기동 시 1회 카테고리·채널 구성(있으면 재사용).

        project_names = 프로젝트 카테고리 채널 목록(코어 list_projects). 특수 채널(간단처리·데이터
        분석·알림·봇상태) 구조는 어댑터가 안다. channelID→역할/폴더 매핑을 영속(channel_map.json).
        """
        ...

    def role_channel(self, role: str) -> int | None:
        """특수 채널 역할("알림"|"봇상태"|…) → channelID(channel_map 역조회). 없으면 None.

        DM 폐기로 알림·재시작완료가 이 채널로 간다. 매핑이 없으면(자동생성 실패) None 을 반환하고
        코어가 해당 발송을 스킵/폴백한다.
        """
        ...

    def project_channel(self, project: str) -> int | None:
        """프로젝트 폴더명 → 그 프로젝트 채널 channelID(channel_map 역조회). 없으면 None.

        예약 확인 실행이 #알림 대신 프로젝트 채널로 스트리밍되게 라우팅에 쓴다(없으면 현 채널 폴백).
        """
        ...

    def clear_channel(self, channel_id: int) -> int:
        """채널의 메시지를 전부 삭제하고 삭제 건수 반환. 파괴적 — 코어가 확인 버튼 뒤에만 호출.

        TextChannel.purge(14일 이내 일괄삭제 + 초과분 개별삭제로 전부 지움). 권한(Manage
        Messages) 없음·오류는 예외를 삼켜 **삭제된 만큼만** 반환(부분 성공 허용)하고 로깅한다.
        """
        ...

    def play_music(self, channel_id: int, user_id: int, query: str = "") -> str:
        """음성재생 capability(디스코드 전용) — 호출자(user_id)가 있는 음성채널에서 고정 재생목록을
        셔플·반복 재생하고 사용자 회신 문자열을 반환한다. 타 어댑터는 미지원(안내 문자열 반환).

        `query`(선택, 'ㅁ재생 <제목>'): 주면 **그 곡을 먼저/지금** 재생한다 — 재생 중이면 현재 곡을
        끊고 그 곡으로, 미재생이면 그 곡부터 시작. 큐에 없는 제목은 유튜브 검색으로 폴백해 한 번만
        튼다(재생목록 자체엔 추가하지 않는다 — 그건 'ㅁ추가' 소관). 안 주면 종전 동작 그대로.
        """
        ...

    def stop_music(self, channel_id: int) -> str:
        """음성재생 정지 + 음성채널 퇴장(디스코드 전용). 회신 문자열 반환. 타 어댑터 미지원."""
        ...

    def skip_music(self, channel_id: int) -> str:
        """현재 곡을 건너뛰고 다음 곡 재생(디스코드 전용). 회신 문자열 반환. 타 어댑터 미지원."""
        ...

    def search_video(self, query: str) -> tuple[str, str] | None:
        """'ㅁ추가 <검색어>' 용 유튜브 검색(yt-dlp ytsearch1) — 첫 결과 (videoId, 제목). 무결과·
        실패는 None. yt-dlp 는 어댑터에만 격리되므로 코어는 이 capability 로 위임한다(음악과 동일).
        """
        ...

    def enqueue_video(self, video_id: str, title: str) -> int:
        """'ㅁ추가' 로 새로 넣은 곡을 재생 중인 큐의 현재 곡 바로 뒤에 편입(디스코드 전용). 편입 후
        큐 곡수를 반환, 재생 중이 아니면 no-op 0. 코어는 >0 일 때만 "▶️ Play - N곡"을 회신에 붙인다.
        """
        ...

    def dequeue_video(self, video_id: str) -> int:
        """'ㅁ삭제' 로 뺀 곡을 재생 중인 큐에서도 제거(디스코드 전용). 제거한 곡수 반환.

        재생 중이 아니거나 큐에 없으면 no-op 0. 지운 곡이 **현재 재생 중이면 다음 곡으로 넘긴다**.
        큐가 통째로 비게 되는 경우엔 재생이 끊기므로 손대지 않고 0(enqueue_video 와 대칭).
        """
        ...


# ── 플랫폼 무관 공유 유틸 ──────────────────────────────────────────────────
# 코어·어댑터가 공유하므로 순환 import 를 피해 이 shared base 에 둔다(bridge 는 재-import 해
# 자기 표면으로 노출 = "코어 잔류" API 유지, 물리 위치만 여기). 플랫폼 라이브러리 의존 없음(stdlib).
def mask_secrets(text: str, secrets: list[str]) -> str:
    """토큰·내부 경로 등 비밀값을 '***'로 치환. 빈 문자열은 무시(텍스트 파괴 방지)."""
    for s in secrets:
        if s:
            text = text.replace(s, "***")
    return text


def fold_title(text: str) -> str:
    """제목 매칭용 정규화(공백접기+casefold). 'ㅁ삭제'(유튜브 목록)와 'ㅁ재생'(재생 큐)이 같은
    입력에 다른 곡을 고르면 안 되므로 **한 벌만** 둔다(youtube·discord_adapter 공용).
    """
    return "".join(text.split()).casefold()


def _valid_id(s: object) -> bool:
    """알림 id 안전 규칙(방출·수신 계약 대칭): 비어있지 않고 ≤54자, [A-Za-z0-9_-] 만.

    이 규칙은 callback_data(nb:ok:<id>·nb:later:<id>) 로 왕복하므로 인바운드(parse_callback)와
    아웃바운드(load_schedules→notify_buttons) 양측이 같은 문을 써야 한다. 상한 54 = callback_data
    캡(100)에서 최장 접두(`nb:handoff:`·`nb:recheck:` 11B)를 빼고도 한참 남는 여유. 길면 render
    절단으로 왕복이 깨져(탭해도 매칭 실패) 방출을 여기서 막는다(근본 차단, 실사용 id 는 짧다).
    """
    return isinstance(s, str) and 0 < len(s) <= 54 and all(c.isalnum() or c in "-_" for c in s)


def chunk_text(text: str, limit: int) -> list[str]:
    """플랫폼 한도(호출측 명시: 어댑터별 상수)로 분할. 빈 문자열이면 [""]."""
    if text == "":
        return [""]
    return [text[i : i + limit] for i in range(0, len(text), limit)]


def _dig(s: str) -> bool:
    """정수 arg 안전 검사(§4.7 재사용): isascii()+isdigit() — 전각·위첨자 유니코드 숫자 차단."""
    return s.isascii() and s.isdigit()


# 예약 알림 버튼 액션(nb:*) — 코덱 단일 소스(디코드·인코드가 같은 목록을 쓴다).
# ok=확인시작 / recheck=다시 확인(판정 불가 재시도, ok 와 동작 동일) / later=스누즈 /
# done=확인완료 1단계(재확인 카드) / confirm=그 진행 / cancel=그 취소 / handoff=이관처리.
# ⚠️ 이름을 바꾸면 **이미 나간 카드의 버튼이 깨진다**(od:rev 와 같은 계약).
_NB_VERBS = ("ok", "recheck", "later", "done", "confirm", "cancel", "handoff")
_NB_ACTIONS = tuple(f"nb:{v}" for v in _NB_VERBS)
_NB_PREFIXES = tuple(f"{a}:" for a in _NB_ACTIONS)


def parse_callback(data: str) -> tuple[str, str] | None:
    """callback_data(신뢰 경계 밖) → (action, arg). 화이트리스트 밖은 None.

    `push`/`x`/`clean:ok` → (그대로, ""), `p:<name>` → ("p", name),
    `nb:<verb>:<id>` → ("nb:<verb>", id) — verb 는 _NB_VERBS 화이트리스트. 정확 매칭만.
    """
    if data in ("push", "x", "clean:ok"):
        return (data, "")
    if data.startswith("p:") and len(data) > 2:
        return ("p", data[2:])
    for prefix in _NB_PREFIXES:
        if data.startswith(prefix):
            item_id = data[len(prefix) :]
            # id 는 우리가 발행하지만 callback_data 는 신뢰 경계 밖 — 방출측과 같은 문(_valid_id).
            if _valid_id(item_id):
                return (prefix[:-1], item_id)
            return None
    if data.startswith("c:"):
        # c:<msg_id>:<idx|other> — msg_id 정수, 선택은 정수 인덱스 또는 'other'.
        # L-3: isascii() 병행으로 전각·위첨자 등 유니코드 숫자(int() 통과)를 차단.
        parts = data.split(":")
        mid_ok = len(parts) == 3 and parts[1].isascii() and parts[1].isdigit()
        sel_ok = mid_ok and (parts[2] == "other" or (parts[2].isascii() and parts[2].isdigit()))
        if sel_ok:
            return ("c", f"{parts[1]}:{parts[2]}")
        return None
    # §4.7 델타3: 후속버튼(②)·매크로(③) 콜백. 전부 정수 arg(isascii+isdigit 재사용) — 라우팅은
    # 1b·1e, 여기선 코덱만. 정확 매칭 밖은 None(폐기) 불변. 코어 _handle_button 은 미분기라
    # "ack 후 무시"로 안전(1a 는 이 버튼들을 방출하지 않음 — 코덱만 준비).
    if data.startswith("r:"):
        # r:<mid> (재실행 확인 게이트) | r:<mid>:go (게이트 통과 실행) | r:<mid>:why (원인 분석).
        # mid 정수, 접미는 정확히 'go'|'why' — 그 밖은 None(폐기) 불변.
        # `why` 는 1b 구현(2026-08-16) 때 추가했다: 계약 §4.2 가 실패 메시지에 `[🔍 원인 분석]`
        # 버튼을 명시하는데 코덱에 자리가 없어 눌러도 죽는 버튼이 될 뻔했다. 읽기전용 진단이라
        # 확인 게이트를 태우지 않는다(사용량이 거의 없고, 게이트가 있으면 아무도 안 누른다).
        parts = data.split(":")
        mid_ok = len(parts) in (2, 3) and _dig(parts[1])
        if mid_ok and len(parts) == 2:
            return ("r", parts[1])
        if mid_ok and len(parts) == 3 and parts[2] in ("go", "why"):
            return ("r", f"{parts[1]}:{parts[2]}")
        return None
    if data.startswith("fav:"):
        # fav:<idx> (실행) | fav:add:<idx> (등록) | fav:del:<idx> (삭제). idx 정수.
        parts = data.split(":")
        if len(parts) == 2 and _dig(parts[1]):
            return ("fav", parts[1])
        if len(parts) == 3 and parts[1] in ("add", "del") and _dig(parts[2]):
            return (f"fav:{parts[1]}", parts[2])
        return None
    if data.startswith("od:"):
        # od:rev:<seq> — 🧩 오픈소스 다이제스트 [🔍N] 버튼(그 레포를 검토·보고). seq = 보류맵 키.
        # 레포명이 아니라 seq 를 싣는 이유 = custom_id 100자 한도(레포명은 길 수 있다).
        # 폐기 이력: `od:skip`(🚫)은 v2 에서 30일 쿨다운이 대신했고, `od:add`(📌 백로그)는
        # 2026-08-02 검토가 흡수했다 — **되살리지 마라**(코어에 핸들러가 없어 ack 후 무시된다).
        parts = data.split(":")
        if len(parts) == 3 and parts[1] == "rev" and _dig(parts[2]):
            return ("od:rev", parts[2])
        return None
    if data.startswith("rec:"):
        # rec:<idx> — 최근 실행. idx 정수.
        idx = data[len("rec:") :]
        if _dig(idx):
            return ("rec", idx)
        return None
    return None


def encode_callback(action: str, arg: str) -> str:
    """정규화 (action, arg) → callback_data 문자열(parse_callback 의 역함수).

    코어 Button.action/arg 를 어댑터 전송 문자열로 직렬화. 디코드(parse_callback)의 역.
    """
    if action in ("push", "x", "clean:ok"):
        return action
    # 콜론-join 액션(§1.3 + §4.7 델타3): arg(id·mid·idx·"mid:idx"·"mid:go")를 그대로 이어붙인다.
    _joined = (
        "p",
        *_NB_ACTIONS,
        "c",
        "r",
        "rec",
        "fav",
        "fav:add",
        "fav:del",
        "od:rev",
    )
    if action in _joined:
        return f"{action}:{arg}"
    return action  # 방출측이 유효 액션만 넘기므로 폴백은 그대로


# ── 사진 다운로드 공유 상수·리다이렉트 차단 opener(어댑터 fetch_file 이 재사용) ──
MAX_PHOTO_BYTES = 10 * 1024 * 1024  # 10MB 상한
PHOTO_EXTS = frozenset({".jpg", ".jpeg", ".png", ".webp"})


# M-3: 리다이렉트 차단 opener. 화이트리스트 CDN 이 3xx 로 내부주소(오라클 메타데이터
# 169.254.169.254 등)를 가리켜도 추종하지 않는다 — redirect_request→None 이면 urllib 이 그 3xx 를
# HTTPError 로 승격해(추종 안 함) 내부주소 재요청을 원천 차단한다. 사진 다운로드(fetch_file)만 이
# opener 를 쓴다 — 고정호스트 REST 는 무관.
class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        # 3xx 를 추종하지 않음 → urllib 이 HTTPError 로 승격(내부주소 재요청 원천 차단).
        return None


_NOREDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler)
