#!/usr/bin/env python3
"""SNS정보 수집 — `#SNS정보` 에 공유된 인스타·X 링크를 옵시디언 수집함 노트로 만든다(순수 부분).

설계 정본은 `docs/기능/SNS정보_수집/01_계획.md` 다. 이 모듈은 **디스코드·어댑터 의존 0**·stdlib 전용
이고, 채널 히스토리 읽기·카드 전송·상태 파일·버튼은 bridge 가 한다(us_digest 와 같은 경계).

- 링크 추출은 **엄격 정규식 + 호스트 정확 일치** — `instagram.com.evil.com` 같은 가짜 도메인과
  `../` 같은 경로 문자는 매칭되지 않는다. 파일명에 들어가는 id 는 `[A-Za-z0-9_-]` 만이다.
- SNS 메시지 본문은 **데이터**다 — 여기서 링크만 뽑고 본문은 저장·로그하지 않는다.
- 중복은 파일명이 아니라 수집함 전 노트의 머리말 `출처` 에서 뽑은 **(플랫폼, 게시물 id)** 로 본다
  (판정 때 파일명이 바뀌고, 같은 게시물도 `/p/`·`/reel/`·사용자명 유무로 주소가 갈린다).
"""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

# 인스타·X 만(계획 §1). 호스트는 정확 일치 — 서브도메인 꼬리(`.evil.com`)를 정규식에 맡기지 않는다.
_INSTA_HOSTS = frozenset({"instagram.com", "www.instagram.com", "m.instagram.com"})
_X_HOSTS = frozenset(
    {"x.com", "www.x.com", "mobile.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"}
)
_URL_RE = re.compile(r"https?://[^\s<>()\"']+", re.IGNORECASE)
# 🔴 숫자는 `\d` 가 아니라 `[0-9]` — `\d` 는 유니코드 숫자(전각·아랍 숫자)도 받는다.
# 문자 클래스는 전부 ASCII 로 명시해 파일명에 들어가는 id 가 `[A-Za-z0-9_-]` 밖으로 못 나간다.
# 인스타: `/<kind>/<CODE>` 또는 `/<아이디>/<kind>/<CODE>`(앱 공유 형태) — 둘 다 CODE 로 정규화.
_INSTA_PATH_RE = re.compile(r"(?:/[A-Za-z0-9_.]{1,30})?/(p|reel|reels|tv)/([A-Za-z0-9_-]{1,64})/?")
# `/share/<토큰>` 은 shortcode 가 아니라 리다이렉트 토큰 — 원래 주소로 저장해 판정 세션이 연다.
_INSTA_SHARE_RE = re.compile(r"/share/(?:(?:p|reel|reels|tv)/)?([A-Za-z0-9_-]{1,64})/?")
# X: `/photo/1`·`/video/1` 꼬리는 허용하고 버린다. `/i/web/status/<id>` 는 사용자명이 없는
# 앱 공유 형태(사용자명을 모를 때 X 가 내는 주소) — 둘 다 받아 같은 (x, id) 중복 키로 묶는다.
_X_PATH_RE = re.compile(
    r"/(?:i/web/status/([0-9]{1,25})|([A-Za-z0-9_]{1,15})/status/([0-9]{1,25}))"
    r"(?:/(?:photo|video)/[0-9]{1,2})?/?"
)

PLATFORM_LABEL = {"insta": "인스타", "x": "X"}


@dataclass(frozen=True)
class Link:
    platform: str  # "insta" | "x"
    post_id: str  # 파일명에 쓰는 id — [A-Za-z0-9_-] 만(정규식이 보장)
    url: str  # 정규 URL(쿼리스트링·조각 제거)
    kind: str = ""  # 중복 키 구분 — 인스타 공유 토큰은 "share"(shortcode 와 다른 이름공간)

    @property
    def key(self) -> tuple[str, str]:
        """중복 비교 키 — (플랫폼, 게시물 id). 주소 모양(사용자명·/p·/reel)과 무관하다."""
        return (f"{self.platform}{':' + self.kind if self.kind else ''}", self.post_id)


def parse_link(raw: str) -> Link | None:
    """URL 하나 → 정규 Link. 인스타·X 게시물 주소가 아니면 None(순수)."""
    try:
        parts = urllib.parse.urlsplit(raw)
    except ValueError:
        return None
    if parts.scheme.lower() not in ("http", "https"):
        return None
    host = (parts.hostname or "").lower()
    path = parts.path
    if any(seg in (".", "..") for seg in path.split("/")):
        return None  # 경로 문자(`../`) — 사용자명 자리(`[A-Za-z0-9_.]`)로 새어 들지 않게
    if host in _INSTA_HOSTS:
        # share 를 먼저 — `/share/reel/<토큰>` 이 «아이디=share» 인 게시물 주소로 오인되지 않게.
        s = _INSTA_SHARE_RE.fullmatch(path)
        if s is not None:
            return Link("insta", s.group(1), f"https://www.instagram.com{path}", kind="share")
        if path.lower().startswith("/share/"):
            return None  # 모르는 share 형태 — 게시물 정규식으로 넘기면 토큰이 shortcode 로 둔갑한다
        m = _INSTA_PATH_RE.fullmatch(path)
        if m is None:
            return None
        kind = "reel" if m.group(1) == "reels" else m.group(1)
        code = m.group(2)
        return Link("insta", code, f"https://www.instagram.com/{kind}/{code}/")
    if host in _X_HOSTS:
        m = _X_PATH_RE.fullmatch(path)
        if m is None:
            return None
        if m.group(1) is not None:  # `/i/web/status/<id>` — 사용자명 없음
            status = m.group(1)
            return Link("x", status, f"https://x.com/i/web/status/{status}")
        user, status = m.group(2).lower(), m.group(3)
        return Link("x", status, f"https://x.com/{user}/status/{status}")
    return None


def extract_links(text: str) -> list[Link]:
    """메시지 본문 → 인스타·X 정규 Link 목록(등장 순서, 메시지 안 중복 제거)."""
    out: dict[tuple[str, str], Link] = {}
    for raw in _URL_RE.findall(text):
        link = parse_link(raw.rstrip(".,!?"))
        if link is not None:
            out.setdefault(link.key, link)
    return list(out.values())


def source_key(value: str) -> tuple[str, str]:
    """머리말 `출처` 값 → 중복 키. 인스타·X 면 (플랫폼, id), 아니면 ("raw", 원문)."""
    value = value.strip().strip("\"'")
    link = parse_link(value)
    return link.key if link is not None else ("raw", value)


def read_source(path: Path) -> tuple[str, str] | None:
    """노트 머리말(`---` 블록)의 `출처:` → 중복 키. 머리말·출처가 없으면 None."""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    for line in lines[1:]:
        if line.strip() == "---":
            break
        key, sep, val = line.partition(":")
        if sep and key.strip() == "출처" and val.strip():
            return source_key(val)
    return None


def known_sources(root: Path) -> set[tuple[str, str]]:
    """수집함(`root`) 아래 **모든** .md 의 중복 키 — 미판정·사용·폐기 어디로 옮겨졌든 잡는다."""
    found: set[tuple[str, str]] = set()
    for p in root.rglob("*.md"):
        src = read_source(p)
        if src:
            found.add(src)
    return found


def has_unsaved_link(text: str, saved: set[tuple[str, str]]) -> bool:
    """본문에 수집함에 **없는** 링크가 하나라도 있나 — #SNS정보 청소가 이 메시지를 남길지(순수).

    인스타·X 가 아닌 링크(스토리·유튜브·threads 등)는 저장 대상이 아니라 늘 «없음» 이다. 인스타·X 는
    중복 키(플랫폼, id)가 saved 에 있어야 «있음». 링크가 없는 메시지(명령·잡담·봇 카드)는 False.
    """
    for raw in _URL_RE.findall(text):
        link = parse_link(raw.rstrip(".,!?"))
        if link is None or link.key not in saved:
            return True
    return False


def note_name(link: Link, shared: datetime) -> str:
    """`<yyyymmdd>_<insta|x>_<id>.md` — id 는 정규식이 [A-Za-z0-9_-] 로 잠근 값뿐."""
    return f"{shared:%Y%m%d}_{link.platform}_{link.post_id}.md"


def free_path(folder: Path, name: str) -> Path:
    """같은 이름이 이미 있으면 `_2`·`_3`… 을 붙인 빈 경로. 인스타 shortcode 는 대소문자를 가리는데
    NTFS 는 안 가린다 — `abc` 와 `ABC` 가 한 파일로 겹쳐 조용히 빠지지 않게 한다.
    """
    path = folder / name
    stem, n = path.stem, 2
    while path.exists():
        path = folder / f"{stem}_{n}.md"
        n += 1
    return path


def note_body(link: Link, shared: datetime, message_id: int) -> str:
    """머리말만 있는 노트 본문. SNS 본문은 받지 않는다(판정 때 읽는다 — 계획 §3-1)."""
    return (
        "---\n"
        f"출처: {link.url}\n"
        f"플랫폼: {PLATFORM_LABEL[link.platform]}\n"
        f"공유시각: {shared:%Y-%m-%d %H:%M}\n"
        f"디스코드메시지: {message_id}\n"
        "상태: 미판정\n"
        "---\n"
    )


def write_atomic(path: Path, text: str) -> None:
    """encode 먼저 → 임시파일 → 원자 교체(Path.replace = os.replace)."""
    data = text.encode("utf-8")
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


# ── 메시지(계획 §2 — 커스텀 메시지 편집기 저장분. 문구를 여기서 바꾸지 마라) ─────────────
# #SNS정보 청소 확인 문구(개발자 채팅 확정 2026-10-10 — 한 줄). 버튼 3개는 bridge 가 붙인다.
SNS_CLEAN_CONFIRM = "🧹 메시지를 청소할까요?"
JUDGE_LABEL = "🔍 판정하기"
JUDGING_LINE = "🖥️ 판정 중"
JUDGED_LINE = "🎉 판정완료"
LAUNCH_FAIL_TEXT = "⛔ VS Code 실행 실패"
NO_FOLDER_TEXT = "⚠️ 수집함 폴더 없음\n➡️ Hachiware/_Obsidian/수집함/미판정\n➡️ 링크 채널 유지"
FAIL_TEXT = "⛔ 저장 실패 - 다음 실행 시 재시도\n➡️ 원인 : claude-bridge/logs/bridge.log"


# 판정완료 카드(계획 §2 «판정완료 카드»·«서식 — B안 + 건수 세 줄»·«카드 나누기» — 개발자 확정본
# 2026-10-10, 글자 그대로). 순서 = 머리 → 건수(머리에 붙이고 세 줄 붙임) → 폐기 목록 → 사용 항목.
JUDGE_HEAD = "### 😎 판정완료"
JUDGE_TOTAL = "총 **{N}**건"
JUDGE_USE_COUNT = "💾 사용 **{n}**건"  # 사용 0건이면 줄째 뺌
JUDGE_MORE = "⚠️ 외 {M}건 수집함 확인"
JUDGE_DROP_COUNT = "🗑 폐기 **{n}**건"  # 폐기 0건이면 줄째 뺌
JUDGE_DROP_ITEM = "**🔴 {제목}**\n> {사유}"  # 이름 굵게 · 바로 아래 인용
JUDGE_USE_ITEM = (
    "### 🟢 {제목}\n**🧾 내용**\n> {무엇}\n**🗂️ 추가**\n> {추가}\n**🛠 사용법**\n> {쓰는법}"
)
JUDGE_USE_CARDS_MAX = 10  # 사용 카드 상한 — 넘으면 사용 줄 바로 아래에 judge_more

# 🔴 칸 값·제목은 SNS 원문을 요약한 외부 유래 문자열 — 서식을 깨거나 가짜 제목·인용을 만들지 않게
# 디스코드 마크다운 기호를 escape 한다. discord.utils.escape_markdown 과 같은 일이지만 이 모듈은
# 디스코드 의존 0(stdlib 전용·어댑터 밖)이라 작은 함수로 둔다. `#`·`>` 는 어디서든 escape 해
# 줄 첫머리(`#`·`-#`·`>`) 경우까지 덮고, 첫머리 `-`·`숫자.`(목록)는 따로 막는다. 값은
# _judge_items 가 이미 한 줄로 접어 `> ` 인용이 한 줄에서 끝난다(테스트로 고정).
# 🔴 `[` 도 escape 한다 — 외부 유래 칸 값이 `[무해한 안내](https://피싱)` 로 **봇 이름을 쓴
# 클릭 가능한 링크**를 만들 수 있다. 디스코드는 `\[text](url)` 을 링크로 파싱하지 않으므로
# `[` 하나로 끝이고 `]`·`(`·`)` 는 필요 없다(보안 점검 2026-10-10).
# 생 URL 자동 링크는 escape 로 막히지 않는다 — 판정_절차.md 가 「링크는 넣지 않는다」로 막는 쪽.
_MD_CHARS = re.compile(r"([\\*_~|`>#\[])")
_MD_LEAD = re.compile(r"^(\s*)(-|\d+\.)")


def escape_md(text: str) -> str:
    def lead(m: re.Match[str]) -> str:
        mark = m.group(2)
        return m.group(1) + ("\\-" if mark == "-" else mark[:-1] + "\\.")

    return _MD_LEAD.sub(lead, _MD_CHARS.sub(r"\\\1", text))


def _esc(item: dict[str, str]) -> dict[str, str]:
    return {k: escape_md(v) for k, v in item.items()}


def _head_card(use: list[dict[str, str]], drop: list[dict[str, str]], drop_shown: int) -> str:
    """한 장 카드(사용 0~1건) 또는 요약 카드(사용 2건+). 묶음 사이 빈 줄 하나, 0건은 뺀다."""
    counts = [JUDGE_HEAD, JUDGE_TOTAL.format(N=len(use) + len(drop))]
    if use:
        counts.append(JUDGE_USE_COUNT.format(n=len(use)))
        if len(use) > JUDGE_USE_CARDS_MAX:
            counts.append(JUDGE_MORE.format(M=len(use) - JUDGE_USE_CARDS_MAX))
    if drop:
        counts.append(JUDGE_DROP_COUNT.format(n=len(drop)))
    blocks = ["\n".join(counts)]  # 큰 제목이 자체 여백을 가져 머리와 건수 사이 빈 줄 없음
    blocks += [JUDGE_DROP_ITEM.format(**_esc(it)) for it in drop[:drop_shown]]
    if len(drop) > drop_shown:  # 길이 초과로 줄인 폐기 — 폐기 목록 끝에 judge_more
        blocks.append(JUDGE_MORE.format(M=len(drop) - drop_shown))
    if len(use) == 1:  # 한 장 카드 — 사용 항목은 폐기 목록 뒤
        blocks.append(JUDGE_USE_ITEM.format(**_esc(use[0])))
    return "\n\n".join(blocks)


# 카드 끝 빈 줄 — 디스코드는 같은 봇의 연속 메시지를 한 덩어리로 붙여 그리고, 끝 개행은 지운다.
# 보이지 않는 글자(U+200B) 한 줄을 붙여야 카드 사이가 한 줄 띄워진다(개발자 10-10).
CARD_GAP = "\n​"


def _cut(text: str, limit: int) -> str:
    """길이로 자른 뒤 꼬리의 홀수 백슬래시 1개를 지운다 — escape 짝(`\\*`)이 반토막 나 그대로
    보이는 `\\` 가 카드 끝에 남지 않게(`CARD_GAP` 이 붙으면 눈에 보인다).
    """
    cut = text[:limit]
    tail = len(cut) - len(cut.rstrip("\\"))
    return cut[:-1] if tail % 2 == 1 else cut


def judge_cards(use: list[dict[str, str]], drop: list[dict[str, str]], limit: int) -> list[str]:
    """«😎 판정완료» 카드 목록(보낼 순서). 항목 0건이면 []. 칸 값은 호출측이 정화·자르기를 마친 것.

    사용 0~1건 = 한 장. 2건+ = 요약 카드 + 사용 1건당 1장(최대 10장, 신호의 항목 순서).
    장마다 limit 을 **escape 뒤 길이로** 지킨다 — 앞 카드가 넘치면 폐기 항목을 뒤에서부터 줄여
    judge_more 로 대신하고,
    사용 카드는 칸 200자 자르기로 넘칠 일이 없지만 방어로 자른다.
    """
    if not use and not drop:
        return []
    drop_shown = len(drop)
    head = _head_card(use, drop, drop_shown)
    while len(head) > limit and drop_shown > 0:
        drop_shown -= 1
        head = _head_card(use, drop, drop_shown)
    cards = [_cut(head, limit)]  # 사용 1건 카드가 폐기를 다 빼고도 넘치면 마지막 방어로 자른다
    if len(use) >= 2:
        cards += [
            _cut(JUDGE_USE_ITEM.format(**_esc(it)), limit) for it in use[:JUDGE_USE_CARDS_MAX]
        ]
    return cards


def _md(d: date) -> str:
    return f"{d.month}/{d.day}"


def card_text(
    insta: int,
    x: int,
    dup: int,
    nolink: int,
    dates: list[date],
    catchup: bool,
) -> str | None:
    """정오 카드 문구. 저장 0건이면 None(sns_none = 보내지 않음).

    catchup = 지난 실행일이 어제보다 이르다(그 사이 실행을 놓쳤다) → sns_catchup(범위 = 저장분의
    공유일), 아니면 sns_daily. 두 경우 모두 sns_skipped 꼬리를 단다(0건인 줄은 뺀다).
    """
    saved = insta + x
    if saved == 0:
        return None
    if catchup and dates:
        first, last = _md(min(dates)), _md(max(dates))
        lines = [f"📥 미처리 {saved}건 수집함 저장", f"➡️ {first} ~ {last} 공유분"]
    else:
        lines = [f"📥 SNS {saved}건 수집함 저장"]
        if insta:
            lines.append(f"➡️ 인스타그램 {insta}건")
        if x:
            lines.append(f"➡️ X {x}건")
    if dup:
        lines.append(f"⛔ 중복 {dup}건 Pass")
    if nolink:
        lines.append(f"⛔ 링크X {nolink}건 Pass")
    return "\n".join(lines)
