#!/usr/bin/env python3
"""미국주식 다이제스트 — `#미국주식` 카드 1장을 조립한다(수집·계산·포매팅 전부).

설계 정본은 `docs/기능/미국주식_다이제스트/01_계획.md` 다. 엔드포인트·필수 헤더·"안 되는 것"이
전부 실측으로 적혀 있고, 이 모듈은 그 결정을 **그대로** 구현한다(재조사 금지).

경계:
- **디스코드·어댑터 의존 0** — 반환값은 `bridge.digest_embed` 와 같은 카드 스펙 dict 라
  코어가 그대로 `adapter.send(card=...)` 로 넘긴다. stdlib 전용.
- **LLM 호출 없음** — 오픈소스 다이제스트와 달리 claude CLI 를 부르지 않는다(순수 수집·포매팅).
- **투자 조언 금지**(계획서 §0·§8) — 매수/매도·목표가 제시·저평가/고평가 판정을 하지 않는다.
  숫자와 **판단이 갈리는 지점**(예: P/E 세 가지가 서로 다르다)만 제시한다.
- **블록 단위 부분 실패 허용** — 죽은 소스는 그 블록만 `조회 실패`로 떨어뜨리고 카드는 낸다.
  **단 MU 시세(Yahoo chart)가 죽으면 `build_us_digest` 가 None** 을 반환한다 — 보유 종목 가격이
  빠진 카드는 낼 이유가 없고, 호출측(bridge._run_digest)이 fired 를 되돌려 다음 틱에 재시도한다.

구조: `_get`/`_json`(네트워크, 실패는 조용히 None) → `parse_*`(순수, 원본 JSON → 값) →
`fmt_*`(순수, 값 → 필드 문자열) → `build_us_digest`(수집 순서·조립). 네트워크와 포매팅이
갈려 있어 `parse_*`·`fmt_*` 는 dict 만으로 테스트된다.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from adapter import _NOREDIRECT_OPENER

log = logging.getLogger("bridge")

PROJECT_DIR = Path(__file__).resolve().parent
LOG_DIR = PROJECT_DIR / "logs"
# SEC companyfacts 원본은 4MB·태그 629종인데 분기 단위로만 바뀐다 → 하루 1회 받아 **요약만**
# 캐시한다(원본을 캐시하면 매 실행 4MB 파싱이 그대로 남는다). gitignore.
SEC_CACHE_FILE = LOG_DIR / "us_sec_facts.json"

# ── 대상 종목(계획서 §2) ───────────────────────────────────────────────────
TICKER = "MU"  # 보유 종목 — 항상 상세. 이 시세가 없으면 카드를 내지 않는다
MU_CIK = "723125"  # SEC EDGAR CIK(무패딩) — 일별 인덱스 경로 매칭·companyfacts 조회에 함께 쓴다
SKHY = "SKHY"  # SK하이닉스 나스닥 — 2026-07-10 상장이라 기간 비교 금지(§2)
KOREA = (("005930.KS", "삼성전자"), ("000660.KS", "SK하이닉스"))
SECTOR = ("NVDA", "AMD", "AVGO", "TSM", "ASML", "ARM", "MRVL", "INTC", "SMCI")
INDEXES = (("^SOX", "SOX"), ("SMH", "SMH"))
FX_SYMBOL = "KRW=X"  # 환율을 빼면 손익이 틀린다(§4-1) — 원화환산의 유일한 재료
VIX_SYMBOL = "^VIX"

# ── 표시 상수 ─────────────────────────────────────────────────────────────
LEAD_US = "📈"
FAIL = "조회 실패"
# 디스코드 field value 한도는 1024, embed 총합은 6000. 필드 8개라 700 x 8 = 5,600 + 제목·footer
# 로 총합 안에 들어온다. **필드를 늘리거나 이 값을 키울 때는 곱을 다시 재라.**
FIELD_MAXLEN = 700
COLOR_UP = 0x3ECF85
COLOR_DOWN = 0xE05A5A
COLOR_FLAT = 0x5865F2
MCAP_TOLERANCE_PCT = 5.0  # SEC 계산 시총 vs Nasdaq 제공 시총 허용 오차(§4-8 교차검증)

# ── HTTP ─────────────────────────────────────────────────────────────────
# 전체 URL 을 받지 않고 **고정 host + 경로**만 조립한다(SSRF 차단 — bridge._digest_get 동형).
_HOSTS = frozenset(
    {
        "query1.finance.yahoo.com",
        "api.nasdaq.com",
        "www.sec.gov",
        "data.sec.gov",
        "apewisdom.io",
        "production.dataviz.cnn.io",
    }
)
_TIMEOUT = 10.0
_MAXBYTES = 8_000_000  # companyfacts 4MB 를 받아야 해서 다이제스트(300KB)보다 크다
# SEC 403 은 **두 가지**이고 뜻이 정반대다(2026-07-29 실측):
#   · 파일 없음(주말·휴일·오타) → S3 가 낸다: `Server: Apache` · `Content-Type: application/xml`
#     · **`x-amz-request-id` 있음**(AccessDenied XML)
#   · 차단(UA 거부·레이트리밋)  → Akamai WAF 가 낸다: `Server: AkamaiGHost` · `text/html`
#     · **`x-amz-request-id` 없음**
# 이 둘을 뭉뚱그려 "없음"으로 보면 **레이트리밋 하루치를 "그날 공시 0건"으로 단언**한다.
# 판별은 본문이 아니라 이 헤더로 한다 — 403 본문은 gzip 으로 와서(identity 를 요청해도) 마커
# 문자열 매칭이 불안정하다.
_S3_ABSENT_HEADER = "x-amz-request-id"
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
# SEC 는 UA 에 **연락 가능한 이메일**을 요구한다 — 이름만·URL만 넣으면 403(2026-07-29 실측).
# 개인 이메일을 코드에 박지 않으려고 `.env` 에서 읽는다. 미설정이면 SEC 블록만 조회 실패로
# 떨어진다(다른 블록은 그대로 나간다).
_ENV_FILE = PROJECT_DIR / ".env"
# CNN 은 UA + Referer + Origin 셋 다 없으면 HTTPError(§1-1).
_CNN_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Referer": "https://edition.cnn.com/",
    "Origin": "https://edition.cnn.com",
}


def _sec_ua() -> str:
    """`.env` 의 SEC_USER_AGENT(`<영문 이름> <이메일>`). 미설정·비ASCII 는 "".

    ⚠️ **한글 이름을 넣으면 안 된다** — HTTP 헤더는 latin-1 이라 `putheader` 가
    UnicodeEncodeError 를 내고, `_get` 이 그걸 삼켜 SEC 블록 3개(펀더멘털·8-K·Form 4)가 **매일**
    `조회 실패`로 나가는데 카드는 멀쩡해 보인다. 여기서 미리 걸러 경고를 남긴다 —
    "조용히 죽는 것"보다 "안 쓴다고 말하고 죽는 것"이 낫다.

    ponytail: `bridge.load_env` 와 같은 형식이지만 여기서 최소 파싱한다 — bridge 가 이 모듈을
    import 하므로 반대로 부르면 순환이다. 값 하나뿐이라 파서를 공유할 이유가 없다.
    """
    ua = ""
    with contextlib.suppress(OSError):
        for raw in _ENV_FILE.read_text(encoding="utf-8").splitlines():
            key, sep, val = raw.strip().partition("=")
            if sep and key.strip() == "SEC_USER_AGENT":
                ua = val.strip().strip('"').strip("'")
                break
    if ua and not ua.isascii():
        log.warning("SEC_USER_AGENT 에 비ASCII 문자 — HTTP 헤더에 못 실어 SEC 블록을 건너뛴다")
        return ""
    return ua


def _get(host: str, path: str, headers: dict[str, str] | None = None) -> bytes | None:
    """allowlist host 에 GET 1회. **`b""` = 서버가 "없다"고 답함 · `None` = 있는지 없는지 모름.**

    이 둘을 가르는 게 이 함수의 계약이다. 뭉뚱그리면 호출측이 조회 실패를 "그날 공시 0건"으로
    오해한다(fetch_daily_index 참조) — SEC Archives 는 S3 백엔드라 **없는 날도 404 가 아니라
    403** 을 준다(실측: 주말 인덱스). 그런데 **차단(레이트리밋·UA 거부)도 403** 이라 코드만으로는
    못 가른다 → `_S3_ABSENT_HEADER` 로 판별한다(그 상수 주석에 실측 근거). 3xx·5xx·타임아웃·
    인코딩 오류는 전부 "모름"이다.

    리다이렉트는 추종하지 않는다(`_NOREDIRECT_OPENER` — bridge._digest_get 과 같은 방어):
    host 를 고정해도 3xx 를 따라가면 그 뒤는 allowlist 밖이고, urllib 은 리다이렉트 때 헤더를
    재전송해 **SEC UA(연락처 이메일)까지 딸려 간다**. 미추종이면 3xx 가 HTTPError 로 승격돼
    위 분기에서 "모름"으로 떨어진다.
    """
    if host not in _HOSTS or not path.startswith("/"):
        return None
    req = urllib.request.Request(
        f"https://{host}{path}",
        method="GET",
        headers=headers or {"User-Agent": _BROWSER_UA, "Accept-Encoding": "identity"},
    )
    try:
        with _NOREDIRECT_OPENER.open(req, timeout=_TIMEOUT) as resp:
            body: bytes = resp.read(_MAXBYTES)
    except urllib.error.HTTPError as exc:
        # 404 = 없음. 403 은 S3(파일 없음)와 WAF(차단)가 같은 코드를 쓰므로 헤더로 가른다.
        absent = exc.code == 404 or (exc.code == 403 and bool(exc.headers.get(_S3_ABSENT_HEADER)))
        log.info(
            "미국주식 조회 %s%.70s HTTP %s (%s)", host, path, exc.code, "없음" if absent else "차단"
        )
        return b"" if absent else None
    except Exception as exc:
        # 그 외 예외(타임아웃·DNS·UA 인코딩 등)는 전부 "모름" — 조용히 흡수하되 없음과 안 섞는다.
        log.info("미국주식 조회 실패 %s%.70s (%s)", host, path, type(exc).__name__)
        return None
    return body


def _json(host: str, path: str, headers: dict[str, str] | None = None) -> Any:
    """allowlist host GET → 파싱된 JSON. 실패·비-JSON 은 None."""
    raw = _get(host, path, headers)
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8", "replace"))
    except ValueError:
        return None


# ── 표시 경계 무해화(순수) ─────────────────────────────────────────────────
# 카드에 실리는 문자열 중 **외부(야후·나스닥·SEC·레딧·CNN)에서 온 것**은 렌더 직전에 여기를
# 지난다. 파서·`_num` 경로에는 걸지 않는다 — 숫자를 망가뜨리고, 위험은 표시에서만 생긴다.
# `|` 도 바꾼다 — `||…||` 스포일러는 **그 사이를 가린다**. Form 4 의 `<rptOwnerName>` 은 제출자가
# 통제하는 값이고 한 필드에 여러 명이 실리므로, 이름 끝과 다음 이름 앞에 `||` 를 넣으면
# `※ S=매도 …` 해석 가드까지 가려진다. `*`·`_`·`~` 는 강조만 만들 뿐 구조를 못 바꿔 그대로 둔다.
_MD_TRANS = str.maketrans({"[": "(", "]": ")", "`": "'", "|": "/"})
# 마크다운 링크로 써도 되는 URL: https(s) + 괄호·공백·따옴표 없음. 괄호가 있으면 `[제목](url)` 의
# 괄호를 **URL 안에서 닫아** 뒤에 두 번째 링크를 붙일 수 있다(라벨·주소가 전부 공격자 통제).
_SAFE_URL_RE = re.compile(r"https?://[^\s()<>\"']{1,300}\Z")


def plain(text: object) -> str:
    """외부 값 → 카드에 실어도 안전한 **한 줄** 문자열. 순수(JSON 이 주는 무엇이든 받는다).

    ① `[`·`]` 를 바꾼다 — 이 둘만 막혀도 마크다운 링크 문법 자체를 만들 수 없다.
    ② 개행·연속 공백을 한 칸으로 접는다 — 개행이 살아남으면 외부 문자열 하나가 **가짜 줄**을
       만들어 `8-K 없음` 같은 우리 표기를 위조한다(bridge.strip_control_line 과 같은 사상).
    ③ None 은 ""(빈 값) — `str(None)` 은 `"None"` 이라 카드에 `(전일 None · 전체 None위)` 처럼
       찍힌다. 외부 응답에 키가 빠지는 일은 흔하다(레딧 신규 추적 티커 실측).
    """
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text).translate(_MD_TRANS)).strip()


def safe_url(url: object) -> str:
    """마크다운 링크에 써도 안전한 URL 만 통과. 아니면 ""(링크 없이 제목만 낸다 — 정보는 안 버린다).

    ⚠️ `str[:200]` 슬라이스는 검증이 아니다 — 길이만 자를 뿐 문법을 못 막는다.
    """
    return str(url) if _SAFE_URL_RE.fullmatch(str(url)) else ""


# ── 공통 포맷 헬퍼(순수) ───────────────────────────────────────────────────
def pct(value: float | None, digits: int = 2) -> str:
    """등락률 문자열(`+1.23%`). None 은 `-`."""
    return "-" if value is None else f"{value:+.{digits}f}%"


def fit(lines: list[str], limit: int = FIELD_MAXLEN) -> str:
    """줄 목록 → 한도 안에 **줄 단위로** 들어가는 문자열. 넘치는 줄은 통째로 버린다.

    글자 수로 자르면 마크다운 링크·괄호가 중간에서 끊겨 깨진 채 표시된다 → 줄 경계에서만 자른다.
    """
    out: list[str] = []
    total = 0
    for line in lines:
        if not line:
            continue
        if total + len(line) + 1 > limit:
            continue
        out.append(line)
        total += len(line) + 1
    return "\n".join(out)


def _num(value: Any) -> float | None:
    """외부 JSON 값 → float. `$1,569.29`·`18.64%`·`36,211,849` 같은 표기도 받는다. 실패는 None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", value.replace("(", "-"))
    with contextlib.suppress(ValueError):
        return float(cleaned)
    return None


def _billions(value: float) -> str:
    return f"{value / 1e9:,.1f}B"


# ── ① 시세(Yahoo chart) ────────────────────────────────────────────────────
def parse_quote(payload: Any) -> dict[str, Any] | None:
    """Yahoo chart JSON → 시세 dict. 형식 이탈은 None. 순수.

    `chartPreviousClose` 는 **조회 창 직전 종가**라 전일 종가가 아니다(range=1y 면 1년 전 값이
    온다 — 실측). 그래서 전일 대비는 **종가 시계열 마지막 두 개**로 계산한다: 카드가 도는 시각
    (KST 아침)엔 미장이 이미 마감돼 있어 `closes[-1]` 이 직전 정규장 종가다.
    """
    try:
        result = payload["chart"]["result"][0]
        meta = result["meta"]
        closes = [
            c for c in result["indicators"]["quote"][0]["close"] if isinstance(c, (int, float))
        ]
    except (KeyError, IndexError, TypeError):
        return None
    if not closes:
        return None
    price = float(closes[-1])
    prev = float(closes[-2]) if len(closes) >= 2 else None
    return {
        "symbol": str(meta.get("symbol") or ""),
        "currency": str(meta.get("currency") or ""),
        "price": price,
        "prev": prev,
        "pct": None if not prev else (price / prev - 1) * 100,
        "w52h": _num(meta.get("fiftyTwoWeekHigh")),
        "w52l": _num(meta.get("fiftyTwoWeekLow")),
        "high": max(closes),  # 조회 창 안 최고 종가 — 상장 직후 종목의 "고점 대비 낙폭"용
        "bars": len(closes),
    }


def fetch_quote(symbol: str, rng: str = "5d") -> dict[str, Any] | None:
    """Yahoo 차트 1회 → 시세 dict. 지수(`^SOX`)·환율(`KRW=X`)·한국주(`005930.KS`) 동일 경로."""
    path = f"/v8/finance/chart/{urllib.parse.quote(symbol)}?range={rng}&interval=1d"
    return parse_quote(_json("query1.finance.yahoo.com", path))


def fmt_price(quote: dict[str, Any], fx: dict[str, Any] | None) -> str:
    """MU 시세 · 52주 위치 · **원화환산**. 환율을 빼면 체감 손익이 틀린다(§4-1)."""
    price = float(quote["price"])
    lines = [f"**${price:,.2f}** {pct(quote.get('pct'))}"]
    prev = quote.get("prev")
    if prev:
        lines[0] += f" (전일 ${float(prev):,.2f})"
    high, low = quote.get("w52h"), quote.get("w52l")
    if high and low and high > low:
        pos = (price - low) / (high - low) * 100
        drop = pct(price / high * 100 - 100, 1)
        lines.append(f"52주 ${low:,.2f}~${high:,.2f} · 위치 {pos:.0f}% · 고점 대비 {drop}")
    if fx and fx.get("price"):
        rate = float(fx["price"])
        lines.append(
            f"원화 ₩{price * rate:,.0f} (환율 {rate:,.2f} {pct(fx.get('pct'))}"
            " — 환율이 손익을 함께 움직인다)"
        )
    else:
        lines.append(f"원화환산 {FAIL}(환율)")
    return fit(lines)


# ── ② 시장 기대(Nasdaq targetprice · earnings-forecast) ─────────────────────
def parse_targetprice(payload: Any) -> dict[str, Any] | None:
    """Nasdaq targetprice JSON → 컨센서스 + **월별 목표가 추이**. `data: null` 이면 None. 순수.

    SKHY 는 이 엔드포인트가 `data: null` 이라(2026-07-29 실측) 목표가 블록을 아예 뺀다.
    """
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None
    overview = data.get("consensusOverview")
    overview = overview if isinstance(overview, dict) else {}
    history: list[tuple[str, float]] = []
    for point in data.get("historicalConsensus") or []:
        if not isinstance(point, dict):
            continue
        when = str((point.get("z") or {}).get("date") or "")  # MM/DD/YYYY
        value = _num(point.get("y"))
        if value is None or len(when.split("/")) != 3:
            continue
        month, _day, year = when.split("/")
        history.append((f"{year}-{month.zfill(2)}", value))
    return {
        "target": _num(overview.get("priceTarget")),
        "buy": overview.get("buy"),
        "hold": overview.get("hold"),
        "sell": overview.get("sell"),
        "history": history,
    }


def parse_forecast(payload: Any) -> dict[str, Any] | None:
    """Nasdaq earnings-forecast JSON → 분기·연간 컨센서스 첫 행. 순수.

    `noOfEstimates` 를 함께 낸다 — SKHY 는 추정인원이 1~2명이라(실측) 같은 무게로 읽으면 안 된다.
    """
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None

    def first(section: str) -> dict[str, Any] | None:
        rows = (
            ((data.get(section) or {}).get("rows")) if isinstance(data.get(section), dict) else None
        )
        return rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else None

    quarter, year = first("quarterlyForecast"), first("yearlyForecast")
    if quarter is None and year is None:
        return None
    return {"quarter": quarter, "year": year}


def fmt_expectation(target: dict[str, Any] | None, forecast: dict[str, Any] | None) -> str:
    """목표가는 **추이**로, 추정치 조정은 **0건도 표기**(§4-2·§4-3)."""
    lines: list[str] = []
    if target and target.get("history"):
        trail = " → ".join(f"{when} ${value:,.0f}" for when, value in target["history"][-3:])
        lines.append(f"목표가 추이 {trail}")
        lines.append(
            f"등급 매수 {target.get('buy')} · 보유 {target.get('hold')} · 매도 {target.get('sell')}"
            + (f" · 현 컨센 ${target['target']:,.0f}" if target.get("target") else "")
        )
    else:
        lines.append(f"목표가 {FAIL}")
    quarter = (forecast or {}).get("quarter") or {}
    if quarter:
        lines.append(
            f"최근 4주 추정치 상향 {quarter.get('up')} · 하향 {quarter.get('down')} "
            f"({quarter.get('fiscalEnd')} 분기 · 추정 {quarter.get('noOfEstimates')}인)"
        )
    else:
        lines.append(f"추정치 조정 {FAIL}")
    lines.append("※ 목표가는 주가를 뒤따라 조정된다 — 절대값이 아니라 추이로 읽는다")
    lines.append("※ 상향·하향 0/0 = 가격만 움직이고 기대치는 그대로(괴리가 벌어진 상태)")
    return fit(lines)


# ── ③ 실적(Nasdaq surprise · calendar) ──────────────────────────────────────
# 분기 발표 주기 근사(일). 회계연도 4분기는 95~100일이라 며칠 어긋난다 → 표시는 항상 `추정`이고
# 이 날짜가 지나면 `미정`으로 떨어진다(fmt_earnings). 정확한 날짜는 캘린더가 채워질 때만 온다.
_EARNINGS_PERIOD_DAYS = 91


def _reported(row: dict[str, Any]) -> date | None:
    """서프라이즈 행의 `dateReported`(`M/D/YYYY`) → date. 못 읽으면 None. 순수."""
    parts = str(row.get("dateReported") or "").split("/")
    if len(parts) != 3:
        return None
    with contextlib.suppress(ValueError):
        return date(int(parts[2]), int(parts[0]), int(parts[1]))
    return None


def parse_surprise(payload: Any) -> list[dict[str, Any]]:
    """Nasdaq earnings-surprise JSON → 분기별 실적 서프라이즈 행을 **최신순으로 정렬해** 반환. 순수.

    ⚠️ **정렬은 여기서 한 번만 한다.** 소비자가 둘(다음 발표일 추정 = 가장 나중 발표일 · 카드
    표시 = 앞 3건)인데 각자 순서를 가정하면 나스닥이 순서를 뒤집는 날 **한 필드 안에서**
    "다음 발표 = 9월 실적 기준"과 "서프라이즈 = 1년 전 분기"가 나란히 인쇄된다.
    (종전 독스트링은 "최신순"이라고 선언만 하고 정렬은 안 했다 — 그 문장을 참으로 만든 것.)
    날짜를 못 읽는 행은 뒤로 민다.
    """
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    table = (data or {}).get("earningsSurpriseTable") if isinstance(data, dict) else None
    rows = (table or {}).get("rows") if isinstance(table, dict) else None
    if not isinstance(rows, list):
        return []
    return sorted(
        (r for r in rows if isinstance(r, dict)),
        key=lambda r: _reported(r) or date.min,
        reverse=True,
    )


def parse_summary_mcap(payload: Any) -> float | None:
    """Nasdaq summary JSON → 시총(§4-8 교차검증용). 형식 이탈은 None. 순수.

    `or {}` 로는 부족하다 — falsy 만 막고 **truthy 쓰레기**(list·str)는 통과시켜 `.get` 에서
    AttributeError 가 난다. 이 값은 카드 조립 본문에서 계산돼 어느 블록 try 에도 안 들어가므로,
    시총 한 줄 때문에 **MU 시세가 멀쩡한데 카드 전체가 죽는다** → 다른 parse_* 와 같은 isinstance
    관용구로 단계마다 확인한다.
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    summary = data.get("summaryData") if isinstance(data, dict) else None
    cell = summary.get("MarketCap") if isinstance(summary, dict) else None
    return _num(cell.get("value")) if isinstance(cell, dict) else None


def parse_calendar(payload: Any, symbols: set[str]) -> list[dict[str, Any]]:
    """Nasdaq 실적캘린더 JSON → 관심 종목만. 먼 미래는 `data: null` 이라 빈 리스트(§1-2). 순수."""
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    rows = (data or {}).get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict) and str(r.get("symbol", "")).upper() in symbols]


def _next_earnings(surprise: list[dict[str, Any]], today: date) -> tuple[str, int] | None:
    """직전 발표일 + 분기 주기 = 다음 발표일 **추정**. 반환 (YYYY-MM-DD, D-day). 실패는 None.

    입력은 parse_surprise 가 이미 최신순으로 정렬해 주지만 여기서도 **max** 를 쓴다 — 정렬되지
    않은 목록(테스트·다른 호출자)이 들어와도 1년 전 날짜로 계산하지 않게(실측 사고: `D--104`).
    ponytail: 캘린더는 근미래만 채워지므로(§1-2) 먼 실적일은 날짜별로 수십 번 훑어야 얻는다 →
    분기 주기 근사로 대신한다. **회계연도 4분기는 95~100일이라 며칠 어긋난다** — 그래서 표시는
    항상 `추정`이고, 추정일이 지나면 호출측이 `미정`으로 떨어뜨린다(없는 확실성을 만들지 않는다).
    """
    reported = [d for d in (_reported(row) for row in surprise) if d is not None]
    if not reported:
        return None
    nxt = max(reported) + timedelta(days=_EARNINGS_PERIOD_DAYS)
    return nxt.isoformat(), (nxt - today).days


def fmt_earnings(
    surprise: list[dict[str, Any]],
    forecast: dict[str, Any] | None,
    calendar: list[dict[str, Any]],
    today: date,
) -> str:
    """다음 발표 D-day + 컨센서스 EPS · 서프라이즈 이력 · 캘린더에 잡힌 관심 종목."""
    lines: list[str] = []
    quarter = (forecast or {}).get("quarter") or {}
    nxt = _next_earnings(surprise, today)
    if nxt is None:
        lines.append(f"다음 발표일 {FAIL}")
    elif nxt[1] < 0:
        # 추정일이 지났는데 서프라이즈 이력이 안 갱신됐다 = 아직 발표 전이거나 이력이 늦은 것.
        # 지난 날짜를 "다음 발표"로 내면 거짓이다 — 모른다고 말한다.
        lines.append(f"{TICKER} 다음 발표 미정 (추정일 {nxt[0]} 경과 — 미발표)")
    else:
        eps = _num(quarter.get("consensusEPSForecast"))
        head = f"{TICKER} 다음 발표 {nxt[0]} 추정 (D-{nxt[1]})"
        if eps is not None:
            head += f" · 컨센 EPS ${eps:,.2f} ({plain(quarter.get('noOfEstimates'))}인)"
        lines.append(head)
    hits = [
        f"{plain(r.get('fiscalQtrEnd'))} {pct(_num(r.get('percentageSurprise')), 1)}"
        for r in surprise[:3]
    ]
    lines.append("서프라이즈 " + (" · ".join(hits) if hits else FAIL))
    if calendar:
        # **날짜를 반드시 붙인다** — 오늘 발표와 어제 발표가 같은 모양으로 나가면 "오늘 일정"으로
        # 오독된다(수집이 오늘·어제 두 날을 합치기 때문).
        lines.append(
            "캘린더 확정 "
            + " · ".join(
                f"{plain(r.get('symbol'))} {str(r.get('day') or '')[5:]} "
                f"{plain(str(r.get('time') or '').replace('time-', ''))}"
                f" 컨센 {plain(r.get('epsForecast'))}"
                for r in calendar[:4]
            )
        )
    return fit(lines)


# ── ④ 펀더멘털(SEC XBRL) ───────────────────────────────────────────────────
_TTM_MAX_SPAN_DAYS = 300  # 최근 4분기 시작~끝 간격 상한(정상 ≈273일) — 넘으면 분기가 빈 것
_Q_MIN, _Q_MAX = 80, 100  # 분기 구간 판정(일)
_A_MIN, _A_MAX = 350, 380  # 연간 구간 판정(일)


def _duration_series(gaap: dict[str, Any], tag: str, unit: str = "USD") -> dict[str, float]:
    """기간형 태그 → `{기간종료일: 값}` 분기 시계열. 순수.

    같은 분기가 여러 filing 에 중복으로 실리므로 **가장 나중에 제출된 값**을 남긴다.
    ⚠️ 회계연도 4분기(MU 는 8월 결산)는 10-K 가 연간만 싣고 분기를 안 실어 **구멍이 난다**
    (실측: 2025-08-28 없음). 그 구멍을 `연간 - 나머지 3분기`로 메운다 — 안 메우면 최근 4분기가
    조용히 한 분기를 건너뛰어 TTM 이 틀린다.
    """
    rows = ((gaap.get(tag) or {}).get("units") or {}).get(unit)
    if not isinstance(rows, list):
        return {}
    quarters: dict[str, tuple[float, str]] = {}
    annuals: list[tuple[str, str, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        start, end, value = row.get("start"), row.get("end"), _num(row.get("val"))
        if not (isinstance(start, str) and isinstance(end, str) and value is not None):
            continue
        try:
            days = (date.fromisoformat(end) - date.fromisoformat(start)).days
        except ValueError:
            continue
        filed = str(row.get("filed") or "")
        if _Q_MIN <= days <= _Q_MAX:
            known = quarters.get(end)
            if known is None or filed >= known[1]:
                quarters[end] = (value, filed)
        elif _A_MIN <= days <= _A_MAX:
            annuals.append((start, end, value))
    out = {end: value for end, (value, _filed) in quarters.items()}
    for start, end, value in annuals:
        if end in out:
            continue
        inner = [v for e, v in out.items() if start < e < end]  # ISO 문자열은 사전순 = 시간순
        if len(inner) == 3:
            out[end] = round(value - sum(inner), 4)
    return out


def _instant_series(gaap: dict[str, Any], tag: str, unit: str = "USD") -> dict[str, float]:
    """시점형 태그(재고·자본) → `{시점: 값}`. 중복은 가장 나중 filing. 순수."""
    rows = ((gaap.get(tag) or {}).get("units") or {}).get(unit)
    if not isinstance(rows, list):
        return {}
    out: dict[str, tuple[float, str]] = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("start"):
            continue
        end, value = row.get("end"), _num(row.get("val"))
        if not isinstance(end, str) or value is None:
            continue
        filed = str(row.get("filed") or "")
        known = out.get(end)
        if known is None or filed >= known[1]:
            out[end] = (value, filed)
    return {end: value for end, (value, _filed) in out.items()}


def parse_sec_facts(payload: Any) -> dict[str, Any] | None:
    """companyfacts(4MB) → 카드가 쓰는 값만 남긴 요약. 형식 이탈은 None. 순수.

    반환 `{"quarters": [{"end","rev","gross","op","net","eps","inv"} ...최근 4], "shares": int}`.
    이 요약만 캐시한다 — 원본을 캐시하면 매 실행 4MB 파싱이 그대로 남는다.
    """
    gaap = (
        ((payload or {}).get("facts") or {}).get("us-gaap") if isinstance(payload, dict) else None
    )
    if not isinstance(gaap, dict):
        return None
    revenue = _duration_series(gaap, "RevenueFromContractWithCustomerExcludingAssessedTax")
    if not revenue:
        revenue = _duration_series(gaap, "Revenues")
    gross = _duration_series(gaap, "GrossProfit")
    operating = _duration_series(gaap, "OperatingIncomeLoss")
    net = _duration_series(gaap, "NetIncomeLoss")
    eps = _duration_series(gaap, "EarningsPerShareDiluted", "USD/shares")
    inventory = _instant_series(gaap, "InventoryNet")
    if not revenue:
        return None
    quarters = [
        {
            "end": end,
            "rev": revenue[end],
            "gross": gross.get(end),
            "op": operating.get(end),
            "net": net.get(end),
            "eps": eps.get(end),
            "inv": inventory.get(end),
        }
        for end in sorted(revenue)[-4:]
    ]
    shares_series = ((payload.get("facts") or {}).get("dei") or {}).get(
        "EntityCommonStockSharesOutstanding"
    )
    shares = 0.0
    if isinstance(shares_series, dict):
        rows = (shares_series.get("units") or {}).get("shares") or []
        dated = [(str(r.get("end")), _num(r.get("val"))) for r in rows if isinstance(r, dict)]
        valid = [(e, v) for e, v in dated if v]
        if valid:
            shares = max(valid)[1] or 0.0
    return {"quarters": quarters, "shares": shares}


def fetch_sec_facts(today: str) -> dict[str, Any] | None:
    """SEC companyfacts 요약. **하루 1회만 원본을 받고** 나머지는 캐시(분기 단위로만 바뀐다)."""
    with contextlib.suppress(OSError, ValueError):
        cached = json.loads(SEC_CACHE_FILE.read_text(encoding="utf-8"))
        if isinstance(cached, dict) and cached.get("day") == today:
            return cached
    ua = _sec_ua()
    if not ua:
        log.info("SEC_USER_AGENT 미설정 — SEC 블록 건너뜀(.env 에 `<이름> <이메일>` 필요)")
        return None
    payload = _json(
        "data.sec.gov",
        f"/api/xbrl/companyfacts/CIK{MU_CIK.zfill(10)}.json",
        {"User-Agent": ua, "Accept-Encoding": "identity"},
    )
    summary = parse_sec_facts(payload)
    if summary is None:
        return None
    summary["day"] = today
    with contextlib.suppress(OSError):
        # tmp→replace 원자적 쓰기(save_notify_state 와 같은 패턴) — 드라이런과 라이브가 겹치면
        # 반쯤 쓰인 JSON 을 읽어 그날 SEC 블록이 통째로 죽는다.
        SEC_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = SEC_CACHE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
        tmp.replace(SEC_CACHE_FILE)
    return summary


def _span_days(quarters: list[dict[str, Any]]) -> int:
    """분기 목록의 첫~끝 간격(일). 날짜를 못 읽으면 0(검사를 건너뛴다 — 합성 픽스처 허용). 순수."""
    ends = [q.get("end") for q in quarters]
    if not all(isinstance(e, str) for e in ends) or len(ends) < 2:
        return 0
    with contextlib.suppress(ValueError):
        return (date.fromisoformat(str(ends[-1])) - date.fromisoformat(str(ends[0]))).days
    return 0


def valuations(
    price: float, facts: dict[str, Any], year_eps: float | None
) -> dict[str, float | None]:
    """P/E 세 가지 — TTM · 최근분기 연율 · 컨센서스(연간). 순수.

    셋을 나란히 두는 이유(§0): 값이 크게 갈리는데 **어느 쪽을 믿을지가 곧 판단**이다.
    메모리처럼 이익이 급변하는 구간에서는 TTM 과 최근분기 연율이 두 배 넘게 벌어진다.

    못 구하면 **None 으로 정직하게 실패**한다(값을 지어내지 않는다):
    - 4분기가 안 차거나 EPS 가 하나라도 비면 TTM 없음
    - 그 4분기가 1년을 넘게 걸쳐 있으면(회계연도 Q4 메움 실패) 15개월치 합이 되므로 TTM 없음
    - `최근분기 연율`은 **가장 최근 분기**에서만 뽑는다 — 결측을 걸러낸 목록의 마지막은
      최근 분기가 아닐 수 있는데 라벨은 "최근분기"라 조용히 거짓이 된다
    """
    quarters = facts.get("quarters") or []
    eps_list = [q.get("eps") for q in quarters if q.get("eps") is not None]
    ttm = sum(eps_list) if len(eps_list) == 4 else None
    if ttm is not None and _span_days(quarters[-4:]) > _TTM_MAX_SPAN_DAYS:
        ttm = None
    last_eps = quarters[-1].get("eps") if quarters else None
    recent = last_eps * 4 if last_eps else None
    return {
        "ttm": price / ttm if ttm else None,
        "recent": price / recent if recent else None,
        "consensus": price / year_eps if year_eps else None,
    }


def fmt_fundamentals(
    facts: dict[str, Any] | None,
    price: float,
    year_eps: float | None,
    nasdaq_mcap: float | None,
) -> tuple[str, str]:
    """SEC 실적 추이 · 재고 사이클 · 밸류 3종. 반환 (필드 문자열, 시총 교차검증 경고).

    재고/매출 비율은 **DRAM 현물가의 부분 대체 지표**다(§1-4) — 현물가는 전부 유료라 못 구하고,
    이건 분기 단위라 반응이 느리다는 한계를 안고 쓴다.
    """
    if not facts or not facts.get("quarters"):
        return f"SEC 재무 {FAIL}", ""
    quarters = list(facts["quarters"])
    lines = ["매출 " + " → ".join(_billions(q["rev"]) for q in quarters[-3:])]
    last = quarters[-1]
    prior = quarters[-2] if len(quarters) >= 2 else {}

    def ratio(quarter: dict[str, Any], key: str) -> str:
        """그 분기 매출 대비 비율(%). 값이 없으면 ""."""
        value = quarter.get(key)
        return "" if not value else f"{value / quarter['rev'] * 100:.1f}%"

    if ratio(last, "gross") and ratio(last, "op"):
        margins = f"이익률 매출총 {ratio(last, 'gross')} · 영업 {ratio(last, 'op')}"
        if ratio(prior, "gross"):
            margins += f" (전분기 {ratio(prior, 'gross')})"
        lines.append(margins)
    if ratio(last, "inv"):
        stock = f"재고/매출 {ratio(last, 'inv')}"
        if ratio(prior, "inv"):
            stock += f" (전분기 {ratio(prior, 'inv')})"
        lines.append(stock + " — DRAM 현물가 대체 지표(분기 단위·반응 느림)")
    val = valuations(price, facts, year_eps)

    def pe(label: str, key: str) -> str:
        """P/E 한 칸. **음수면 `(적자)` 를 붙인다** — 숫자만 보면 낮은 배수로 오독된다."""
        value = val[key]
        return "" if not value else f"{label} {value:.1f}" + ("(적자)" if value < 0 else "")

    parts = [
        pe("TTM", "ttm") or f"TTM {FAIL}",
        pe("최근분기 연율", "recent"),
        pe("컨센", "consensus"),
    ]
    lines.append("P/E " + " · ".join(p for p in parts if p))
    warn = ""
    shares = facts.get("shares") or 0
    if shares and nasdaq_mcap:
        sec_mcap = shares * price
        gap = (sec_mcap / nasdaq_mcap - 1) * 100
        lines.append(
            f"시총 SEC {_billions(sec_mcap)} vs Nasdaq {_billions(nasdaq_mcap)} ({pct(gap, 1)})"
        )
        if abs(gap) > MCAP_TOLERANCE_PCT:
            warn = f"⚠️ 시총 교차검증 불일치 {pct(gap, 1)} — 재무 수치 확인 필요"
    lines.append("※ 경기민감주는 이익 정점에서 P/E 가 가장 낮게 나온다(알려진 특성)")
    return fit(lines), warn


# ── ⑤ 수급·심리(공매도 · Form 4 · 레딧 · 공포탐욕 · VIX) ────────────────────
def parse_short_interest(payload: Any) -> dict[str, Any] | None:
    """Nasdaq short-interest JSON → 최신 2회 잔고. 순수."""
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    table = (data or {}).get("shortInterestTable") if isinstance(data, dict) else None
    rows = (table or {}).get("rows") if isinstance(table, dict) else None
    if not isinstance(rows, list) or not rows:
        return None
    latest = rows[0] if isinstance(rows[0], dict) else {}
    prior = rows[1] if len(rows) > 1 and isinstance(rows[1], dict) else {}
    return {
        "date": latest.get("settlementDate"),
        "interest": _num(latest.get("interest")),
        "days_to_cover": _num(latest.get("daysToCover")),
        "prior": _num(prior.get("interest")),
    }


def parse_apewisdom(payload: Any, ticker: str) -> dict[str, Any] | None:
    """ApeWisdom JSON → 그 티커의 레딧 언급량·순위. 없으면 None. 순수."""
    rows = (payload or {}).get("results") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return None
    for row in rows:
        if isinstance(row, dict) and str(row.get("ticker", "")).upper() == ticker:
            return row
    return None


def parse_fear_greed(payload: Any) -> dict[str, Any] | None:
    """CNN 공포탐욕 JSON → 현재 지수·직전 종가. 순수."""
    index = (payload or {}).get("fear_and_greed") if isinstance(payload, dict) else None
    return index if isinstance(index, dict) else None


def fmt_flows(
    short: dict[str, Any] | None,
    form4: list[dict[str, str]] | None,
    reddit: dict[str, Any] | None,
    fear: dict[str, Any] | None,
    vix: dict[str, Any] | None,
    form4_day: str = "",
) -> str:
    """공매도·내부자·레딧·공포탐욕·VIX. Form 4 는 **매도 우위를 악재로 읽지 말 것**을 함께 낸다.

    `form4_day` = 그 Form 4 가 실린 인덱스 날짜. 인덱스가 하루 이상 거슬러 올라갔을 때
    **이틀 전 내부자거래가 오늘 것처럼 보이는 것**을 막는다(붙일 날짜가 없으면 생략).
    """
    lines: list[str] = []
    if short:
        head = f"공매도 {short['interest']:,.0f}주" if short.get("interest") else f"공매도 {FAIL}"
        if short.get("days_to_cover") is not None:
            head += f" · daysToCover {short['days_to_cover']:.1f}"
        if short.get("prior"):
            head += f" (직전 {short['prior']:,.0f}주)"
        if short.get("date"):
            head += f" · {plain(short['date'])} 결제"
        lines.append(head)
    else:
        lines.append(f"공매도 {FAIL}")
    when = f" ({form4_day})" if form4_day else ""
    if form4 is None:
        lines.append(f"Form 4 {FAIL}")
    elif not form4:
        lines.append(f"Form 4 없음{when}")
    else:
        # 한 사람이 같은 날 여러 건을 내는 일이 흔하다(실측 7/28 CEO 2건) → 표시는 중복 제거,
        # 건수는 원래대로.
        who = " · ".join(dict.fromkeys(f"{plain(f['owner'])}({plain(f['codes'])})" for f in form4))
        lines.append(f"Form 4 {len(form4)}건{when} — {who}")
        lines.append("※ S=매도 M=옵션행사 P=매수 A=부여 F=세금납부. 매도 우위가 곧 악재는 아니다")
    if reddit:
        lines.append(
            f"레딧 {plain(reddit.get('ticker'))} 언급 {plain(reddit.get('mentions'))}건 "
            f"(전일 {plain(reddit.get('mentions_24h_ago'))} · 전체 {plain(reddit.get('rank'))}위)"
        )
    else:
        lines.append(f"레딧 언급 {FAIL}")
    tail = []
    if fear and fear.get("score") is not None:
        # 전일값은 **있을 때만** 붙인다 — 공포탐욕에서 0 은 결측이 아니라 "극단적 공포"라는
        # 실값이라, `or 0` 으로 채우면 하루 만에 극단공포→중립으로 튄 것처럼 읽힌다.
        prior = _num(fear.get("previous_close"))
        tail.append(
            f"공포탐욕 {float(fear['score']):.0f} {plain(fear.get('rating'))}"
            + (f" (전일 {prior:.0f})" if prior is not None else "")
        )
    if vix:
        tail.append(f"VIX {vix['price']:.2f} {pct(vix.get('pct'), 1)}")
    lines.append(" · ".join(tail) if tail else f"심리지표 {FAIL}")
    return fit(lines)


# ── ⑥ 공시·뉴스(SEC 일별 인덱스 1회 · Yahoo 뉴스) ───────────────────────────
_IDX_FORM_WIDTH = 12  # form.idx 는 고정폭 — 앞 12칸이 Form Type
_FORM4_OWNER_RE = re.compile(r"<rptOwnerName>([^<]{1,60})</rptOwnerName>")
_FORM4_CODE_RE = re.compile(r"transactionCode>([A-Z])<")
_FORM4_AMEND_RE = re.compile(r"<documentType>\s*4/A\s*</documentType>")  # 정정본 표시(실측 확인)
_FORM4_MAX = 4  # 원문을 열어볼 Form 4 상한(하루 몇 건 안 되지만 폭주 대비)


def parse_daily_index(text: str, cik: str) -> dict[str, Any]:
    """SEC 일별 인덱스 → 그 CIK 의 8-K·Form 4 행. 순수.

    **호출 1회로 8-K 와 Form 4 를 동시에 얻는다**(§8) — 종목별 submissions 조회(160KB x N)보다
    훨씬 싸고, Form 4 는 사실상 공짜로 딸려온다(§4-4).
    CIK 는 무패딩 정수로 실리고 파일 경로에도 `edgar/data/<cik>/` 로 들어간다 —
    고정폭 컬럼을 세는 것보다 **경로 매칭**이 안전하다(회사명 길이에 안 흔들린다).

    데이터줄 판별도 같은 이유로 **경로 유무**로 한다. 머리말 낱말 목록으로 걸러내던 옛 방식은
    header 가 **두 줄로 접혀** 오는 실제 응답(`      Date Filed  File Name`)을 못 걸러 `total` 이
    항상 1 컸다 — 카드에 "전체 6,030건"이라고 쓰던 값이 실제로는 6,029 였다.

    ⚠️ **`total == 0` 은 "그날 공시가 없었다"가 아니라 파싱 실패 신호다** — 미국 전체 공시가
    0건인 날은 없다. SEC 가 200 으로 점검·오류 HTML 을 주면 여기서 0 이 나오므로 **호출측
    (fetch_daily_index)이 조회 실패로 승격**한다. 이 함수는 셈만 하고 판정하지 않는다.
    """
    marker = f"/{cik}/"
    total = 0
    eightk: list[str] = []
    form4: list[str] = []
    for line in text.splitlines():
        if "edgar/data/" not in line:
            continue
        form = line[:_IDX_FORM_WIDTH].strip()
        total += 1
        if marker not in line:
            continue
        if form.startswith("8-K"):
            eightk.append(line.split()[-1])
        elif form.split("/")[0] == "4":
            # 정정본은 항상 `<코드>/A` 라 앞부분만 본다 — `4` 와 `4/A` 둘 다 내부자거래다.
            # ⚠️ `startswith("4")` 로 하면 `40-F`(외국기업 연차)·`424B2`·`497` 이 딸려 와
            # 내부자거래로 오집계된다. 반드시 `/` 앞 **정확일치**로 자를 것.
            form4.append(line.split()[-1])
    return {"total": total, "8-K": eightk, "4": form4}


def fetch_daily_index(today: str, cik: str) -> dict[str, Any] | None:
    """가장 최근에 존재하는 일별 인덱스(오늘 KST 기준 전일부터 역순 4일) 1건. 전부 실패면 None.

    카드가 도는 시각(KST 아침)은 미 동부 전일 저녁이라 **전일치 인덱스**가 그날의 공시 전량이다.
    주말·휴일이면 그 날짜가 없으므로 최대 4일 거슬러 첫 성공에서 멈춘다.

    ⚠️ **역행은 "서버가 없다고 답한 날"에만 한다.** 요청이 한 번 타임아웃 났다고 거슬러 올라가면
    그날을 "공시 없는 날"로 단정하는 셈이라, 실제로 8-K 가 있었어도 카드에
    `8-K 없음 (전전일 …)` 이 나간다 — 조회 실패는 조회 실패라고 말한다(None).
    """
    ua = _sec_ua()
    if not ua:
        return None
    day = date.fromisoformat(today)
    for back in range(1, 5):
        target = day - timedelta(days=back)
        quarter = (target.month - 1) // 3 + 1
        path = (
            f"/Archives/edgar/daily-index/{target.year}/QTR{quarter}/"
            f"form.{target.strftime('%Y%m%d')}.idx"
        )
        raw = _get("www.sec.gov", path, {"User-Agent": ua, "Accept-Encoding": "identity"})
        if raw is None:  # 있는지 없는지 모름 → 역행하지 않는다
            log.info("미국주식 일별 인덱스 %s 조회 실패 — 역행 없이 중단", target)
            return None
        if raw:
            found = parse_daily_index(raw.decode("utf-8", "replace"), cik)
            if not found["total"]:
                # 데이터줄 0 = form.idx 가 아닌 것을 받았다(200 + 점검·오류 HTML). 그대로 두면
                # 카드에 `8-K 없음 (전체 0건 중 해당 없음)` 이라는 **없는 사실**이 실린다.
                log.info("미국주식 일별 인덱스 %s 형식 이탈(데이터줄 0) — 조회 실패로 본다", target)
                return None
            found["day"] = target.isoformat()
            return found
    return None


def fetch_form4_details(paths: list[str]) -> list[dict[str, str]]:
    """Form 4 원문에서 보고자·거래코드·정정 여부를 뽑는다(없으면 건수만 남는다).

    정정본(`4/A`)은 **원문이 스스로 밝힌다**(`<documentType>4/A</documentType>` — 실측). 인덱스에서
    따로 들고 오지 않아도 되고, 정정을 신규 거래로 읽는 오독(§4-4 주석보다 큰 오독)을 막는다.

    `rel` 은 인덱스 줄의 마지막 토큰이라 **그 포맷이 바뀌면 무관한 sec.gov 경로**가 될 수 있다 →
    이 종목 폴더 밖은 받지 않는다(엉뚱한 사람 이름이 내부자거래로 실리는 것을 막는다).
    ⚠️ `startswith` 만으로는 부족하다 — `edgar/data/723125/../../x` 가 통과한다. urllib 은 경로를
    정규화하지 않고 **원문 그대로 보내고 S3 가 정규화**해서(실측) 폴더 밖 파일이 실제로 온다.
    """
    ua = _sec_ua()
    out: list[dict[str, str]] = []
    for rel in paths[:_FORM4_MAX]:
        if ".." in rel or not rel.startswith(f"edgar/data/{MU_CIK}/"):
            log.info("미국주식 Form 4 경로 이탈 — 건너뜀")
            continue
        raw = _get(
            "www.sec.gov", f"/Archives/{rel}", {"User-Agent": ua, "Accept-Encoding": "identity"}
        )
        if not raw:
            continue
        text = raw.decode("utf-8", "replace")
        owners = _FORM4_OWNER_RE.findall(text)
        codes = sorted(set(_FORM4_CODE_RE.findall(text)))
        mark = "·정정" if _FORM4_AMEND_RE.search(text) else ""
        out.append({"owner": owners[0] if owners else "?", "codes": ("".join(codes) or "?") + mark})
    return out


def parse_news(payload: Any, limit: int = 3) -> list[dict[str, str]]:
    """Yahoo 뉴스 검색 JSON → 헤드라인·출처·링크. 순수."""
    rows = (payload or {}).get("news") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    out: list[dict[str, str]] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "")
        out.append(
            {
                "title": title[:90] + ("…" if len(title) > 90 else ""),
                "publisher": str(row.get("publisher") or "")[:30],
                "link": str(row.get("link") or "")[:200],
            }
        )
    return out


def fmt_filings(index: dict[str, Any] | None, news: list[dict[str, str]]) -> str:
    """8-K 유무 + 헤드라인. **"8-K 없음"도 정보다**(§4-6) — 회사 사건이 아니라 분위기였다는 뜻."""
    lines: list[str] = []
    if index is None:
        lines.append(f"8-K {FAIL}")
    elif index.get("8-K"):
        lines.append(f"8-K {len(index['8-K'])}건 ({index.get('day')} 접수)")
    else:
        lines.append(f"8-K 없음 ({index.get('day')} 전체 {index.get('total', 0):,}건 중 해당 없음)")
    for item in news:
        # 제목·출처는 무해화, URL 은 **통과 못 하면 링크를 아예 안 만든다** — 괄호가 든 URL 은
        # `[제목](url)` 을 URL 안에서 닫아 그 뒤에 라벨·주소가 전부 남의 것인 **가짜 링크**를
        # 신뢰받는 봇 카드에 띄울 수 있다(피싱). 거절돼도 제목·출처는 그대로 남는다.
        title, source = plain(item["title"]), plain(item["publisher"])
        url = safe_url(item["link"])
        lines.append(f"· [{title}]({url}) — {source}" if url else f"· {title} — {source}")
    if not news:
        lines.append(f"뉴스 {FAIL}")
    return fit(lines)


# ── ⑦ 한국 메모리 3사 · ⑧ 섹터 ────────────────────────────────────────────
_SKHY_MAX_BARS = 60  # 이 미만이면 상장 직후로 보고 `[상장 N일차]` 를 붙인다


def fmt_korea(
    quotes: dict[str, dict[str, Any] | None], skhy_forecast: dict[str, Any] | None = None
) -> str:
    """한국장이 먼저 열린다 = 그날 미장의 선행 신호(§4-5).

    SKHY 는 **목표가가 없다**(targetprice 가 `data: null` — 실측) → 그 블록은 아예 안 낸다.
    EPS 컨센서스는 있지만 추정인원이 1~2명이라 MU(10~13명)와 같은 무게로 읽으면 안 된다 →
    **인원을 반드시 병기**한다.
    """
    lines: list[str] = []
    for symbol, label in KOREA:
        quote = quotes.get(symbol)
        if quote:
            lines.append(f"{label} ₩{quote['price']:,.0f} {pct(quote.get('pct'))}")
        else:
            lines.append(f"{label} {FAIL}")
    skhy = quotes.get(SKHY)
    if skhy:
        tail = f"{SKHY} ${skhy['price']:,.2f} {pct(skhy.get('pct'))}"
        # 상장 13거래일(실측)이라 거래량 배수·기간 비교는 가짜 정밀도다(§2) → 고점 대비 낙폭만.
        if skhy.get("bars", 0) < _SKHY_MAX_BARS:
            tail += f" [상장 {skhy['bars']}일차]"
        if skhy.get("high"):
            tail += f" · 고점 대비 {pct(skhy['price'] / skhy['high'] * 100 - 100, 1)}"
        lines.append(tail)
    else:
        lines.append(f"{SKHY} {FAIL}")
    year = (skhy_forecast or {}).get("year") or {}
    eps = _num(year.get("consensusEPSForecast"))
    if eps is not None:
        lines.append(
            f"{SKHY} 컨센 EPS ${eps:,.2f} ({plain(year.get('fiscalEnd'))} · 추정 "
            f"{plain(year.get('noOfEstimates'))}인 — 인원이 적어 대표성은 낮다)"
        )
    lines.append("※ 한국장이 미장보다 먼저 열린다 — 같은 회사도 두 시장에서 다르게 움직인다")
    return fit(lines)


def fmt_sector(quotes: dict[str, dict[str, Any] | None]) -> str:
    """지수 2종 + 반도체·AI 9종 등락 한 줄 요약.

    죽은 종목은 목록에서 빼되 **몇 종이 빠졌는지는 남긴다** — 9종이 조용히 2종으로 줄면 읽는
    사람은 "오늘 섹터는 이게 다"로 읽는다. 9칸을 전부 `조회 실패`로 채우면 700자를 먹으므로
    꼬리 한 칸으로만 알린다.
    """

    def move(symbol: str, label: str) -> str:
        quote = quotes.get(symbol)
        return f"{label} {FAIL}" if quote is None else f"{label} {pct(quote.get('pct'), 1)}"

    head = " · ".join(move(symbol, label) for symbol, label in INDEXES)
    moves = [move(s, s) for s in SECTOR if quotes.get(s) is not None]
    missing = len(SECTOR) - len(moves)
    body = " · ".join([*moves, f"({missing}종 {FAIL})"] if missing else moves)
    return fit([head, body])


# ── 조립 ──────────────────────────────────────────────────────────────────
def _field(name: str, text: str) -> tuple[str, str, bool]:
    """Embed field 1개(포매팅 실패도 카드를 죽이지 않게 최종 방어)."""
    return (name, text[:FIELD_MAXLEN] or FAIL, False)


def _safe(name: str, build: Any, *args: Any) -> str:
    """블록 하나를 만든다 — 실패는 `조회 실패`로 떨어뜨리고 카드는 계속 만든다(부분 실패 허용)."""
    try:
        return build(*args) or FAIL
    except Exception as exc:
        log.info("미국주식 %s 블록 실패(%s)", name, type(exc).__name__)
        return FAIL


def build_us_digest(today: str) -> dict[str, Any] | None:
    """미국주식 다이제스트 카드 스펙 1장. **MU 시세를 못 받으면 None**(호출측이 재시도).

    수집은 전부 순차다 — 30여 회 GET 이 20~30초 걸리지만 데몬 스레드에서 돌아 타이머를 막지 않는다.
    ponytail: 병렬화는 각 API 의 rate limit 을 모르는 상태에서 위험만 늘린다. 느려서 문제가 되면
    Yahoo 시세 17건만 스레드풀로 묶는다.
    """
    day = date.fromisoformat(today)
    mu = fetch_quote(TICKER, "1y")
    if mu is None:
        log.warning("미국주식 %s 시세 조회 실패 — 카드를 내지 않는다(다음 틱 재시도)", TICKER)
        return None

    # ① 시세 묶음(환율·VIX·한국·섹터). SKHY 만 상장일수·고점이 필요해 3개월 창으로 받는다.
    quotes: dict[str, dict[str, Any] | None] = {SKHY: fetch_quote(SKHY, "3mo")}
    for symbol in (
        FX_SYMBOL,
        VIX_SYMBOL,
        *(s for s, _ in KOREA),
        *(s for s, _ in INDEXES),
        *SECTOR,
    ):
        quotes[symbol] = fetch_quote(symbol)
    fx = quotes.get(FX_SYMBOL)

    # ② Nasdaq — 목표가·컨센서스·서프라이즈·공매도·시총.
    target = parse_targetprice(_json("api.nasdaq.com", f"/api/analyst/{TICKER}/targetprice"))
    forecast = parse_forecast(_json("api.nasdaq.com", f"/api/analyst/{TICKER}/earnings-forecast"))
    surprise = parse_surprise(_json("api.nasdaq.com", f"/api/company/{TICKER}/earnings-surprise"))
    # SKHY 는 목표가(targetprice)가 `data: null` 이라 컨센서스만 받는다(계획서 §7 결정 2).
    skhy_forecast = parse_forecast(
        _json("api.nasdaq.com", f"/api/analyst/{SKHY}/earnings-forecast")
    )
    short = parse_short_interest(
        _json("api.nasdaq.com", f"/api/quote/{TICKER}/short-interest?assetClass=stocks")
    )
    nasdaq_mcap = parse_summary_mcap(
        _json("api.nasdaq.com", f"/api/quote/{TICKER}/summary?assetclass=stocks")
    )
    watch = {TICKER, SKHY, *SECTOR}
    # 오늘·어제 두 날을 훑되 **종목당 하나**만 남긴다(같은 종목이 양쪽에 걸리면 중복 표시).
    # 오늘 것을 먼저 넣어 setdefault 가 오늘을 이기게 한다. 날짜는 표시에 쓰이므로 함께 심는다.
    by_symbol: dict[str, dict[str, Any]] = {}
    for back in (0, 1):
        when = (day - timedelta(days=back)).isoformat()
        rows = parse_calendar(_json("api.nasdaq.com", f"/api/calendar/earnings?date={when}"), watch)
        for row in rows:
            by_symbol.setdefault(str(row.get("symbol")), {**row, "day": when})
    calendar = list(by_symbol.values())

    # ③ SEC — 재무 요약(하루 캐시) + 일별 인덱스 1회(8-K·Form 4 동시).
    facts = fetch_sec_facts(today)
    index = fetch_daily_index(today, MU_CIK)
    form4: list[dict[str, str]] | None = None  # None = 인덱스를 못 받음(≠ 0건)
    if index is not None:
        paths = index.get("4") or []
        details = fetch_form4_details(paths)
        # **건수는 인덱스가 이미 안다.** 원문 조회가 실패하거나 상한(_FORM4_MAX)에 잘려도
        # 건수는 유지한다 — 부족분을 `?` 로 채운다. 안 채우면 있던 공시가 "없음"으로 나간다.
        form4 = details + [{"owner": "?", "codes": "?"}] * (len(paths) - len(details))

    # ④ 심리·뉴스.
    reddit = parse_apewisdom(_json("apewisdom.io", "/api/v1.0/filter/all-stocks/page/1"), TICKER)
    fear = parse_fear_greed(
        _json("production.dataviz.cnn.io", "/index/fearandgreed/graphdata", _CNN_HEADERS)
    )
    news = parse_news(
        _json(
            "query1.finance.yahoo.com",
            f"/v1/finance/search?q={TICKER}&newsCount=5&quotesCount=0",
        )
    )

    # 이 블록만 반환이 튜플(필드 + footer 경고)이라 _safe 를 못 쓴다 → 같은 태도로 직접 감싼다.
    year_eps = _num(((forecast or {}).get("year") or {}).get("consensusEPSForecast"))
    try:
        fundamentals, warn = fmt_fundamentals(facts, float(mu["price"]), year_eps, nasdaq_mcap)
    except Exception as exc:
        log.info("미국주식 펀더멘털 블록 실패(%s)", type(exc).__name__)
        fundamentals, warn = f"SEC 재무 {FAIL}", ""
    change = mu.get("pct")
    fields = [
        _field("💵 MU 시세", _safe("시세", fmt_price, mu, fx)),
        _field("🎯 시장 기대", _safe("기대", fmt_expectation, target, forecast)),
        _field("📅 실적", _safe("실적", fmt_earnings, surprise, forecast, calendar, day)),
        _field("🏭 펀더멘털(SEC)", fundamentals),
        _field(
            "🔄 수급·심리",
            _safe(
                "수급",
                fmt_flows,
                short,
                form4,
                reddit,
                fear,
                quotes.get(VIX_SYMBOL),
                str((index or {}).get("day") or ""),
            ),
        ),
        _field("📰 공시·뉴스", _safe("공시", fmt_filings, index, news)),
        _field("🇰🇷 한국 메모리 3사", _safe("한국", fmt_korea, quotes, skhy_forecast)),
        _field("🧠 섹터", _safe("섹터", fmt_sector, quotes)),
    ]
    footer = "Yahoo · Nasdaq · SEC XBRL · ApeWisdom · CNN · 판단 재료 제공(투자 조언 아님)"
    return {
        "title": f"{LEAD_US} 미국주식 {today} · {TICKER} ${mu['price']:,.2f} {pct(change)}",
        "fields": fields,
        "footer": f"{warn} · {footer}" if warn else footer,
        "color": COLOR_FLAT if not change else (COLOR_UP if change > 0 else COLOR_DOWN),
    }


def _selftest() -> None:
    """순수 함수 자가검증 — 네트워크 없이 파싱·포매팅 계약만 본다."""
    assert pct(1.234) == "+1.23%" and pct(None) == "-"
    assert fit(["가" * 10, "나" * 10], 12) == "가" * 10  # 넘치는 줄은 통째로 버린다
    assert _num("$1,569.29") == 1569.29 and _num("18.64%") == 18.64 and _num("bad") is None
    quote = parse_quote(
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "MU",
                            "fiftyTwoWeekHigh": 1000.0,
                            "fiftyTwoWeekLow": 100.0,
                        },
                        "indicators": {"quote": [{"close": [900.0, None, 800.0]}]},
                    }
                ]
            }
        }
    )
    assert quote is not None
    assert (quote["price"], quote["prev"]) == (800.0, 900.0)  # 창 직전 종가가 아니라 시계열로
    assert abs(quote["pct"] + 11.111) < 0.01
    assert parse_quote({"chart": {"result": []}}) is None
    # 회계연도 4분기 구멍(10-K 는 연간만 싣는다) → `연간 - 3분기`로 메운다.
    gaap = {
        "X": {
            "units": {
                "USD": [
                    {"start": "2024-09-01", "end": "2024-11-30", "val": 1.0, "filed": "2024-12-01"},
                    {"start": "2024-12-01", "end": "2025-02-28", "val": 2.0, "filed": "2025-03-01"},
                    {"start": "2025-03-01", "end": "2025-05-31", "val": 3.0, "filed": "2025-06-01"},
                    {
                        "start": "2024-09-01",
                        "end": "2025-08-31",
                        "val": 10.0,
                        "filed": "2025-10-01",
                    },
                ]
            }
        }
    }
    assert _duration_series(gaap, "X")["2025-08-31"] == 4.0
    facts = {"quarters": [{"eps": 1.0}, {"eps": 2.0}, {"eps": 3.0}, {"eps": 4.0}], "shares": 10}
    val = valuations(100.0, facts, 20.0)
    assert (val["ttm"], val["recent"], val["consensus"]) == (10.0, 6.25, 5.0)
    idx = (
        "Form Type   Company Name                            CIK\n"
        "-------------------------------------------------------\n"
        "4                MICRON TECHNOLOGY INC   723125   20260728   edgar/data/723125/a.txt\n"
        "8-K              OTHER CORP              999999   20260728   edgar/data/999999/b.txt\n"
    )
    found = parse_daily_index(idx, "723125")
    assert (found["total"], found["4"], found["8-K"]) == (2, ["edgar/data/723125/a.txt"], [])
    assert "8-K 없음" in fmt_filings({"day": "2026-07-28", "total": 2, "8-K": []}, [])
    assert FAIL in fmt_fundamentals(None, 1.0, None, None)[0]
    # 인덱스를 못 받은 것(None)과 공시가 실제로 없는 것([])은 다른 사실이다.
    assert f"Form 4 {FAIL}" in fmt_flows(None, None, None, None, None)
    assert "Form 4 없음" in fmt_flows(None, [], None, None, None)
    # 표시 경계 — 링크 문법을 만들 수 없어야 하고, 괄호 든 URL 은 링크가 되면 안 된다.
    assert plain("a[b](c)\nd") == "a(b)(c) d"
    assert safe_url("https://ok.example/a") and not safe_url("https://x/a) [피싱](https://y")
    assert not safe_url("javascript:alert(1)")
    print("us_digest selftest ok")


if __name__ == "__main__":
    _selftest()
