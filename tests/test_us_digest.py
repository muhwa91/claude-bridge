"""미국주식 다이제스트(`us_digest.py`) 단위·통합 테스트 + bridge 배선 검증.

**네트워크 호출 0.** 모든 통합 테스트는 유일한 네트워크 seam 인 `us_digest._get` 을 픽스처
바이트로 갈아끼운다(`_json` 은 `_get` 위에 얹혀 있어 seam 이 하나다). 픽스처는 계획서 §1-1 에
적힌 **실제 응답 모양을 축약**한 것이며 형태(키 이름·중첩·문자열 표기)는 바꾸지 않았다.

무엇을 지키려는 테스트인가:
- 조용히 틀리는 숫자 — 회계연도 Q4 구멍(TTM), 조회 창 직전 종가(전일 대비), 4분기 미만 TTM
- **거짓 표기** — `조회 실패`(못 받음)와 `없음`(그날 공시 0건)이 섞이면 카드가 거짓말을 한다
- 부분 실패 — 소스 하나가 죽어도 카드는 나가되, MU 시세만은 없으면 카드를 내지 않는다
- 디스코드 한도 — field 1024 · embed 총합 6000
"""

import json
import urllib.error
from datetime import date, datetime

import bridge
import pytest
import us_digest
from us_digest import (
    FAIL,
    _duration_series,
    _instant_series,
    _next_earnings,
    _num,
    build_us_digest,
    fit,
    fmt_filings,
    fmt_flows,
    fmt_fundamentals,
    fmt_korea,
    fmt_price,
    fmt_sector,
    parse_apewisdom,
    parse_calendar,
    parse_daily_index,
    parse_fear_greed,
    parse_forecast,
    parse_news,
    parse_quote,
    parse_sec_facts,
    parse_short_interest,
    parse_surprise,
    parse_targetprice,
    valuations,
)

# 디스코드 하드 한도(어댑터가 아니라 플랫폼이 정한 값) — 카드가 이걸 넘으면 게시 자체가 400.
DISCORD_FIELD_MAX = 1024
DISCORD_EMBED_TOTAL_MAX = 6000


@pytest.fixture(autouse=True)
def _isolate_sec_cache(monkeypatch, tmp_path):
    """SEC 요약 캐시를 tmp 로 — 라이브 `logs/us_sec_facts.json` 오염 방지(conftest 와 같은 가드)."""
    monkeypatch.setattr(us_digest, "SEC_CACHE_FILE", tmp_path / "us_sec_facts.json")


def _line(text: str, prefix: str) -> str:
    """포맷 결과에서 그 접두어로 시작하는 줄 1개(없으면 "")."""
    return next((ln for ln in text.split("\n") if ln.startswith(prefix)), "")


# ═══════════════════════════════════════════════════════════════════════════
# ① 순수 파서 — parse_quote
# ═══════════════════════════════════════════════════════════════════════════
def _chart(closes, **meta):
    base = {"symbol": "MU", "currency": "USD", "chartPreviousClose": 95.0}
    base.update(meta)
    return {
        "chart": {
            "result": [
                {
                    "meta": base,
                    "timestamp": list(range(len(closes))),
                    "indicators": {"quote": [{"close": list(closes), "volume": [1] * len(closes)}]},
                }
            ],
            "error": None,
        }
    }


def test_parse_quote_ignores_chart_previous_close():
    # range=1y 면 chartPreviousClose 는 **1년 전** 종가다(실측). 전일 대비는 시계열 마지막 둘로.
    quote = parse_quote(_chart([100.0, 900.0, 820.0], chartPreviousClose=95.0))
    assert quote is not None
    assert (quote["price"], quote["prev"]) == (820.0, 900.0)
    assert quote["prev"] != 95.0
    assert abs(quote["pct"] - (820.0 / 900.0 - 1) * 100) < 1e-9


def test_parse_quote_skips_none_closes():
    # 휴장·데이터 결손 봉은 close=null 로 온다 → 그 자리를 전일로 쓰면 등락률이 통째로 틀린다.
    quote = parse_quote(_chart([900.0, None, None, 800.0]))
    assert quote is not None
    assert (quote["price"], quote["prev"], quote["bars"]) == (800.0, 900.0, 2)


def test_parse_quote_all_none_closes_is_none():
    assert parse_quote(_chart([None, None])) is None


@pytest.mark.parametrize(
    "payload",
    [
        {"chart": {"result": []}},  # 심볼 오타·상장폐지
        {"chart": {"result": None}},
        {"chart": None},
        {},
        None,
        "not json object",
        {"chart": {"result": [{"meta": {}}]}},  # indicators 없음
        {"chart": {"result": [{"meta": {}, "indicators": {"quote": []}}]}},
    ],
)
def test_parse_quote_malformed_is_none(payload):
    assert parse_quote(payload) is None


def test_parse_quote_single_bar_has_no_prev():
    # 상장 첫날처럼 봉이 하나뿐이면 전일 대비는 **없는 것**이지 0%가 아니다.
    quote = parse_quote(_chart([500.0]))
    assert quote is not None
    assert quote["prev"] is None and quote["pct"] is None


def test_parse_quote_high_is_window_max_not_52w():
    # SKHY(상장 직후)는 52주 값이 없어 "고점 대비"를 조회 창 최고 종가로 낸다.
    quote = parse_quote(_chart([10.0, 30.0, 20.0]))
    assert quote is not None
    assert quote["high"] == 30.0


def test_parse_quote_reads_52w_from_meta():
    quote = parse_quote(_chart([820.53], fiftyTwoWeekHigh=1213.56, fiftyTwoWeekLow=61.54))
    assert quote is not None
    assert (quote["w52h"], quote["w52l"]) == (1213.56, 61.54)


# ═══════════════════════════════════════════════════════════════════════════
# ② 순수 파서 — _duration_series (회계연도 Q4 구멍)
# ═══════════════════════════════════════════════════════════════════════════
def _row(start, end, val, filed):
    return {"start": start, "end": end, "val": val, "filed": filed, "form": "10-Q"}


def _gaap(rows, tag="Revenues", unit="USD"):
    return {tag: {"label": tag, "units": {unit: rows}}}


# MU 8월 결산 FY2025: 10-Q 3건 + 10-K(연간). 4분기(2025-08-31)는 어디에도 분기로 안 실린다.
_MU_FY = [
    _row("2024-09-01", "2024-11-30", 1.0, "2024-12-20"),
    _row("2024-12-01", "2025-02-28", 2.0, "2025-03-20"),
    _row("2025-03-01", "2025-05-31", 3.0, "2025-06-20"),
    _row("2024-09-01", "2025-08-31", 10.0, "2025-10-10"),  # 10-K 연간
]


def test_duration_series_fills_fiscal_q4_hole():
    # 안 메우면 최근 4분기가 조용히 한 분기를 건너뛰어 TTM 이 틀린다(_duration_series docstring).
    series = _duration_series(_gaap(_MU_FY), "Revenues")
    assert series["2025-08-31"] == 4.0  # 연간 10 - (1+2+3)
    assert sorted(series) == ["2024-11-30", "2025-02-28", "2025-05-31", "2025-08-31"]


def test_duration_series_q4_hole_fill_survives_row_shuffle():
    # SEC 는 filing 순서를 보장하지 않는다 — 연간이 먼저 와도 결과가 같아야 한다.
    shuffled = [_MU_FY[3], _MU_FY[1], _MU_FY[0], _MU_FY[2]]
    assert _duration_series(_gaap(shuffled), "Revenues")["2025-08-31"] == 4.0


def test_duration_series_does_not_overwrite_real_q4():
    # 회사가 Q4 를 분기로도 실었다면 그 값이 정답 — 뺄셈 추정으로 덮어쓰면 안 된다.
    rows = [*_MU_FY, _row("2025-06-01", "2025-08-31", 9.0, "2025-09-20")]
    assert _duration_series(_gaap(rows), "Revenues")["2025-08-31"] == 9.0


def test_duration_series_skips_fill_when_quarters_incomplete():
    # 분기가 2개뿐이면 뺄셈이 성립하지 않는다 → 가짜 숫자를 만들지 않고 그냥 비운다.
    rows = [_MU_FY[0], _MU_FY[1], _MU_FY[3]]
    assert "2025-08-31" not in _duration_series(_gaap(rows), "Revenues")


def test_duration_series_prefers_latest_filing_for_duplicate_quarter():
    # 같은 분기가 10-Q·10-K 에 중복으로 실린다(재작성 포함) → 나중 filing 이 이긴다.
    rows = [
        _row("2025-03-01", "2025-05-31", 3.5, "2025-09-01"),  # 나중 filing 이 먼저 등장
        _row("2025-03-01", "2025-05-31", 3.0, "2025-06-20"),
    ]
    assert _duration_series(_gaap(rows), "Revenues")["2025-05-31"] == 3.5


@pytest.mark.parametrize(
    "days_end",
    [
        "2024-10-15",  # 44일 — 2개월 미만
        "2025-02-28",  # 180일 — 반기(누적 YTD 공시)
        "2025-06-30",  # 302일 — 3분기 누적
        "2025-11-30",  # 455일 — 연간보다 김
    ],
)
def test_duration_series_rejects_odd_durations(days_end):
    # 반기·누적(YTD) 구간이 분기 시계열에 섞이면 합계·TTM 이 통째로 틀린다.
    rows = [_row("2024-09-01", days_end, 7.0, "2025-01-01")]
    assert _duration_series(_gaap(rows), "Revenues") == {}


@pytest.mark.parametrize(
    "gaap",
    [
        {},
        {"Revenues": {}},
        {"Revenues": {"units": {}}},
        {"Revenues": {"units": {"USD": "nope"}}},
        {"Revenues": {"units": {"USD": ["not a dict", 3]}}},
    ],
)
def test_duration_series_malformed_is_empty(gaap):
    assert _duration_series(gaap, "Revenues") == {}


def test_duration_series_skips_unparseable_dates():
    rows = [_row("2024-13-99", "2025-02-28", 1.0, "2025-03-01")]
    assert _duration_series(_gaap(rows), "Revenues") == {}


def test_instant_series_ignores_duration_rows_and_takes_latest_filing():
    # 재고는 시점형 — start 가 있는 행(기간형)이 섞이면 안 된다.
    rows = [
        {"end": "2025-05-31", "val": 100.0, "filed": "2025-06-20"},
        {"end": "2025-05-31", "val": 111.0, "filed": "2025-09-01"},
        _row("2025-03-01", "2025-05-31", 999.0, "2025-10-01"),
    ]
    assert _instant_series(_gaap(rows, "InventoryNet"), "InventoryNet") == {"2025-05-31": 111.0}


# ═══════════════════════════════════════════════════════════════════════════
# ③ 순수 계산 — valuations (4분기 미만이면 TTM 없음)
# ═══════════════════════════════════════════════════════════════════════════
def test_valuations_three_metrics():
    facts = {"quarters": [{"eps": 1.0}, {"eps": 2.0}, {"eps": 3.0}, {"eps": 4.0}]}
    val = valuations(100.0, facts, 20.0)
    assert (val["ttm"], val["recent"], val["consensus"]) == (10.0, 6.25, 5.0)


def test_valuations_ttm_none_when_four_quarters_span_over_a_year():
    # 회계연도 Q4 메움에 실패하면 `최근 4개`가 15개월치가 된다 — 그걸 TTM 이라 부르면 거짓이다.
    gapped = {
        "quarters": [
            {"end": "2025-05-31", "eps": 1.0},
            {"end": "2025-11-30", "eps": 1.0},  # 중간 분기 누락
            {"end": "2026-02-28", "eps": 1.0},
            {"end": "2026-05-31", "eps": 1.0},
        ]
    }
    assert valuations(100.0, gapped, None)["ttm"] is None


def test_valuations_recent_comes_from_latest_quarter_only():
    # 라벨이 "최근분기"인데 결측을 걸러낸 목록의 마지막(=옛 분기)을 쓰면 조용히 거짓이 된다.
    facts = {"quarters": [{"eps": 5.0}, {"eps": 4.0}, {"eps": 3.0}, {"eps": None}]}
    assert valuations(100.0, facts, None)["recent"] is None


def test_valuations_ttm_none_below_four_quarters():
    # 3분기만 있으면 TTM 은 **없는 것**이다 — 3분기 합으로 낸 P/E 는 조용히 33% 싸 보인다.
    facts = {"quarters": [{"eps": 1.0}, {"eps": 2.0}, {"eps": 3.0}]}
    assert valuations(100.0, facts, None)["ttm"] is None


def test_valuations_ttm_none_when_one_eps_missing():
    facts = {"quarters": [{"eps": 1.0}, {"eps": None}, {"eps": 3.0}, {"eps": 4.0}]}
    val = valuations(100.0, facts, None)
    assert val["ttm"] is None
    # 최근분기 연율은 마지막 값만 있으면 낼 수 있다 — 100 / (4.0 x 4)
    assert val["recent"] == 6.25


def test_valuations_all_none_when_facts_empty():
    val = valuations(100.0, {"quarters": []}, None)
    assert val == {"ttm": None, "recent": None, "consensus": None}


def test_valuations_zero_eps_does_not_divide_by_zero():
    facts = {"quarters": [{"eps": 0.0}, {"eps": 0.0}, {"eps": 0.0}, {"eps": 0.0}]}
    val = valuations(100.0, facts, 0.0)
    assert val == {"ttm": None, "recent": None, "consensus": None}


def test_valuations_loss_quarters_yield_negative_pe_marked_as_loss():
    # 메모리 다운사이클(적자)에서도 죽지 않는다. 계산은 음수 그대로 두되,
    # **표시에는 `(적자)` 를 붙인다** — `-25.0` 만 보면 낮은 배수(저평가)로 오독된다.
    facts = {"quarters": [{"eps": -1.0}] * 4, "shares": 0}
    assert valuations(100.0, facts, None)["ttm"] == -25.0

    def _q(eps: float) -> list[dict]:
        return [{"end": f"2026-0{i + 1}-28", "rev": 1e10, "eps": eps} for i in range(4)]

    assert "P/E TTM -25.0(적자)" in fmt_fundamentals({"quarters": _q(-1.0)}, 100.0, None, None)[0]
    assert "(적자)" not in fmt_fundamentals({"quarters": _q(1.0)}, 100.0, None, None)[0]


# ═══════════════════════════════════════════════════════════════════════════
# ④ 순수 파서 — parse_daily_index (헤더 제외 · CIK 경로 매칭)
# ═══════════════════════════════════════════════════════════════════════════
# ⚠️ **실제 응답 그대로**(2026-07-28 실측). 핵심은 컬럼 머리글이 **두 줄로 접혀** 온다는 것 —
# 아홉 번째 줄 `      Date Filed  File Name` 이 그것이다. 이 픽스처를 한 줄로 펴 놓으면
# "머리말 낱말로 걸러내기" 같은 구현이 통과해 버리고, 실전에서는 그 줄이 데이터로 세어져
# `total` 이 항상 1 커진다(카드에 "전체 6,030건"으로 나가던 값의 실제는 6,029).
_IDX_HEADER = (
    "Description:           Daily Index of EDGAR Dissemination Feed by Form Type\n"
    "Last Data Received:    July 28, 2026\n"
    "Comments:              webmaster@sec.gov\n"
    "Anonymous FTP:         ftp://ftp.sec.gov/edgar/\n"
    "\n"
    " \n"
    " \n"
    " \n"
    "Form Type   Company Name                                                  CIK\n"
    "      Date Filed  File Name\n"
    "------------------------------------------------------------------------------\n"
)


def _idx(*rows: str) -> str:
    return _IDX_HEADER + "".join(rows)


def _idx_row(form: str, name: str, cik: str, path: str) -> str:
    return f"{form:<12}{name:<32}{cik:<11}20260728    {path}\n"


def test_parse_daily_index_excludes_header_lines_from_total():
    # 머리말만 있는 응답의 total 은 0 이어야 한다 — 접힌 둘째 머리글 줄까지 포함해서.
    found = parse_daily_index(_idx(), "723125")
    assert found == {"total": 0, "8-K": [], "4": []}


def test_parse_daily_index_total_counts_only_data_rows():
    # `total` 은 카드에 "전체 N건 중 해당 없음"으로 **그대로 인쇄**되는 값이라 1건도 틀리면 안 된다.
    found = parse_daily_index(
        _idx(
            _idx_row("8-K", "A CORP", "111", "edgar/data/111/a.txt"),
            _idx_row("4", "B PERSON", "222", "edgar/data/222/b.txt"),
        ),
        "723125",
    )
    assert found["total"] == 2


def test_parse_daily_index_html_error_page_counts_nothing():
    # SEC 는 차단 시 200 에 HTML 을 준다 — 태그 줄을 공시로 세면 "전체 40건"처럼 지어낸다.
    html = "<html>\n<body>\n<h1>Access Denied</h1>\n</body>\n</html>\n"
    assert parse_daily_index(html, "723125") == {"total": 0, "8-K": [], "4": []}


def test_parse_daily_index_matches_cik_by_path():
    found = parse_daily_index(
        _idx(
            _idx_row("4", "MEHROTRA SANJAY", "1234567", "edgar/data/723125/a.txt"),
            _idx_row("4", "MURPHY MARK J", "7654321", "edgar/data/723125/b.txt"),
            _idx_row("8-K", "MICRON TECHNOLOGY INC", "723125", "edgar/data/723125/c.txt"),
            _idx_row("8-K", "OTHER CORP", "999999", "edgar/data/999999/d.txt"),
        ),
        "723125",
    )
    assert found["total"] == 4  # 그날 전체 건수는 남의 공시까지 센다("N건 중 해당 없음" 표기용)
    assert found["4"] == ["edgar/data/723125/a.txt", "edgar/data/723125/b.txt"]
    assert found["8-K"] == ["edgar/data/723125/c.txt"]


def test_parse_daily_index_cik_match_is_not_substring():
    # CIK 1723125 는 723125 가 아니다 — 경로 마커가 `/cik/` 라 앞자리 오염에 안 걸려야 한다.
    found = parse_daily_index(
        _idx(
            _idx_row("8-K", "DECOY CORP", "1723125", "edgar/data/1723125/x.txt"),
            _idx_row("4", "DECOY TWO", "7231250", "edgar/data/7231250/y.txt"),
        ),
        "723125",
    )
    assert found["8-K"] == [] and found["4"] == [] and found["total"] == 2


def test_parse_daily_index_company_name_with_spaces_keeps_path():
    # 회사명이 길고 공백이 많아도 경로는 항상 마지막 토큰이다(고정폭 컬럼 세기보다 안전).
    row = (
        "8-K         A B C D E F G HOLDINGS INC   723125     20260728    edgar/data/723125/z.txt\n"
    )
    assert parse_daily_index(_idx(row), "723125")["8-K"] == ["edgar/data/723125/z.txt"]


def test_parse_daily_index_counts_amendments_for_both_8k_and_form4():
    # 정정본(`<코드>/A`)도 원본과 같이 센다. 안 세면 그날 정정본만 들어온 경우 카드에
    # "Form 4 없음"이라는 **거짓**이 나간다(계획서 §4-6 "없음은 그 자체로 정보다"와 충돌).
    found = parse_daily_index(
        _idx(
            _idx_row("8-K/A", "MICRON TECHNOLOGY INC", "723125", "edgar/data/723125/a1.txt"),
            _idx_row("4/A", "MEHROTRA SANJAY", "1234567", "edgar/data/723125/a2.txt"),
        ),
        "723125",
    )
    assert found["8-K"] == ["edgar/data/723125/a1.txt"]
    assert found["4"] == ["edgar/data/723125/a2.txt"]


def test_parse_daily_index_form4_match_excludes_other_forms_starting_with_4():
    # 이번 수정의 진짜 함정 — `startswith("4")` 로 자르면 아래가 전부 내부자거래로 오집계된다.
    # `40-F`=외국기업 연차보고 · `424B2`/`424B3`=증권신고 보충 · `497`=투자회사 서류.
    found = parse_daily_index(
        _idx(
            _idx_row("40-F", "MICRON TECHNOLOGY INC", "723125", "edgar/data/723125/b1.txt"),
            _idx_row("424B3", "MICRON TECHNOLOGY INC", "723125", "edgar/data/723125/b2.txt"),
            _idx_row("497", "MICRON TECHNOLOGY INC", "723125", "edgar/data/723125/b3.txt"),
            _idx_row("4", "MEHROTRA SANJAY", "1234567", "edgar/data/723125/b4.txt"),
        ),
        "723125",
    )
    assert found["4"] == ["edgar/data/723125/b4.txt"]  # 진짜 Form 4 한 건만
    assert found["total"] == 4


def test_parse_daily_index_empty_text():
    assert parse_daily_index("", "723125") == {"total": 0, "8-K": [], "4": []}


# ═══════════════════════════════════════════════════════════════════════════
# ⑤ 순수 헬퍼 — fit · _num
# ═══════════════════════════════════════════════════════════════════════════
def test_fit_drops_whole_line_never_cuts_markdown_link():
    link = "· [메모리 3사 실적 발표, 기대치 하회](https://example.com/a/b/c/d) — Reuters"
    out = fit([link], len(link) - 1)
    assert out == ""  # 조각이 아니라 통째로 사라진다
    assert "](" not in out and "http" not in out


def test_fit_keeps_link_intact_when_it_fits():
    link = "· [제목](https://example.com/x) — Reuters"
    assert fit([link], len(link) + 1) == link


def test_fit_skips_oversized_line_but_keeps_later_short_ones():
    # `break` 가 아니라 `continue` — 긴 줄 하나 때문에 뒤의 주석(※)까지 날아가면 안 된다.
    assert fit(["A" * 10, "B" * 10, "C"], 13) == "A" * 10 + "\nC"


def test_fit_drops_empty_lines():
    assert fit(["", "A", "", "B"], 100) == "A\nB"


def test_fit_never_exceeds_limit():
    lines = [f"{i:03d} " + "가" * 50 for i in range(40)]
    assert len(fit(lines, us_digest.FIELD_MAXLEN)) <= us_digest.FIELD_MAXLEN


def test_field_slice_is_noop_because_fit_shares_the_same_default():
    """실제 자르기는 **fit 이 줄 경계에서만** 한다 — `_field` 의 슬라이스는 no-op 이어야 한다.

    위 fit 테스트는 전부 limit 을 명시로 넘겨서, `fit` 의 기본값이 FIELD_MAXLEN 에서 어긋나도
    아무 테스트가 안 깨진다(변이 검증에서 실제로 생존). 어긋나면 `_field` 의 `[:FIELD_MAXLEN]`
    가 대신 자르는데 그건 **글자 단위 컷**이라 마크다운 링크가 URL 한복판에서 끊긴다 —
    fit 이 막으려던 바로 그 현상이 조용히 돌아온다. 공시·뉴스 필드가 실측 674/700 이라
    여유가 26자뿐이어서 남 얘기가 아니다.
    """
    line = "y" * (us_digest.FIELD_MAXLEN // 2 + 10)  # 두 줄이면 기본 한도를 넘는다
    text = fit([line, line])  # limit 미지정 = 기본값
    assert text == line  # 둘째 줄은 통째로 버려진다
    assert us_digest._field("이름", text)[1] == line  # 슬라이스가 아무것도 안 자른다


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ("$1,234.56", 1234.56),
        ("1,569.29", 1569.29),
        ("18.64%", 18.64),
        ("(1,234.56)", -1234.56),  # 회계 괄호 = 음수
        ("($0.50)", -0.5),
        ("-8.85", -8.85),
        ("36,211,849", 36211849.0),
        (0, 0.0),
        (12, 12.0),
        (3.5, 3.5),
        ("", None),
        ("N/A", None),
        ("--", None),
        ("-", None),
        (None, None),
        (True, None),  # bool 은 int 서브클래스라 명시 차단(1.0 으로 새면 조용히 틀린다)
        (False, None),
        ([1], None),
        ({"v": 1}, None),
    ],
)
def test_num_parsing(raw, want):
    assert _num(raw) == want


# ═══════════════════════════════════════════════════════════════════════════
# ⑥ 나머지 순수 파서 (Nasdaq · ApeWisdom · CNN · Yahoo 뉴스)
# ═══════════════════════════════════════════════════════════════════════════
_TARGETPRICE = {
    "data": {
        "symbol": "MU",
        "consensusOverview": {"priceTarget": "1,569.29", "buy": 29, "hold": 1, "sell": 0},
        "historicalConsensus": [
            {"x": 1, "y": "1035.50", "z": {"date": "06/01/2026", "value": "1035.50"}},
            {"x": 2, "y": "1569.29", "z": {"date": "07/01/2026", "value": "1569.29"}},
        ],
    },
    "message": None,
    "status": {"rCode": 200},
}


def test_parse_targetprice_history_normalized_to_yyyy_mm():
    parsed = parse_targetprice(_TARGETPRICE)
    assert parsed is not None
    assert parsed["history"] == [("2026-06", 1035.50), ("2026-07", 1569.29)]
    assert (parsed["target"], parsed["buy"], parsed["hold"], parsed["sell"]) == (1569.29, 29, 1, 0)


def test_parse_targetprice_skips_broken_history_points():
    payload = {
        "data": {
            "consensusOverview": {},
            "historicalConsensus": [
                {"y": "1.0", "z": {"date": "2026-07-01"}},  # 구분자 다름
                {"y": "bad", "z": {"date": "07/01/2026"}},
                "not a dict",
                {"y": "2.0", "z": {"date": "07/01/2026"}},
            ],
        }
    }
    parsed = parse_targetprice(payload)
    assert parsed is not None and parsed["history"] == [("2026-07", 2.0)]


@pytest.mark.parametrize("payload", [{"data": None}, {}, None, {"data": []}, "x"])
def test_parse_targetprice_null_data_is_none(payload):
    # SKHY 는 이 엔드포인트가 `data: null`(실측) → 목표가 블록을 통째로 뺀다.
    assert parse_targetprice(payload) is None


_FORECAST = {
    "data": {
        "symbol": "MU",
        "quarterlyForecast": {
            "rows": [
                {
                    "fiscalEnd": "Aug 2026",
                    "consensusEPSForecast": "14.20",
                    "noOfEstimates": "12",
                    "up": "0",
                    "down": "0",
                }
            ]
        },
        "yearlyForecast": {
            "rows": [{"fiscalEnd": "Aug 2026", "consensusEPSForecast": "43.50", "up": 2, "down": 0}]
        },
    }
}


def test_parse_forecast_takes_first_row_of_each_section():
    parsed = parse_forecast(_FORECAST)
    assert parsed is not None
    assert parsed["quarter"]["consensusEPSForecast"] == "14.20"
    assert parsed["year"]["fiscalEnd"] == "Aug 2026"


@pytest.mark.parametrize(
    "payload",
    [
        {"data": None},
        {"data": {"quarterlyForecast": {"rows": []}, "yearlyForecast": {"rows": []}}},
        {"data": {"quarterlyForecast": None, "yearlyForecast": None}},
        None,
    ],
)
def test_parse_forecast_empty_is_none(payload):
    assert parse_forecast(payload) is None


def test_parse_surprise_and_calendar_filter():
    surprise = parse_surprise(
        {
            "data": {
                "earningsSurpriseTable": {
                    "rows": [
                        {"fiscalQtrEnd": "May2026", "dateReported": "6/25/2026", "eps": "12.30"},
                        "junk",
                    ]
                }
            }
        }
    )
    assert [r["fiscalQtrEnd"] for r in surprise] == ["May2026"]
    calendar = parse_calendar(
        {
            "data": {
                "rows": [
                    {"symbol": "nvda", "time": "time-after-hours", "epsForecast": "$1.20"},
                    {"symbol": "KO", "time": "time-pre-market"},
                    "junk",
                ]
            }
        },
        {"MU", "NVDA"},
    )
    assert [r["symbol"] for r in calendar] == ["nvda"]  # 대소문자 무관 매칭


@pytest.mark.parametrize("payload", [{"data": None}, {}, None, {"data": {"rows": None}}])
def test_parse_calendar_empty(payload):
    assert parse_calendar(payload, {"MU"}) == []


@pytest.mark.parametrize("payload", [{"data": None}, {}, None])
def test_parse_surprise_empty(payload):
    assert parse_surprise(payload) == []


def test_next_earnings_estimates_from_last_report():
    assert _next_earnings([{"dateReported": "6/25/2026"}], date(2026, 7, 29)) == ("2026-09-24", 57)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{"dateReported": "2026-06-25"}],  # 구분자 다름
        [{"dateReported": ""}],
        [{}],
    ],
)
def test_next_earnings_none_on_bad_dates(rows):
    assert _next_earnings(rows, date(2026, 7, 29)) is None


def test_next_earnings_skips_impossible_date_and_uses_next_row():
    rows = [{"dateReported": "13/45/2026"}, {"dateReported": "6/25/2026"}]
    assert _next_earnings(rows, date(2026, 7, 29))[0] == "2026-09-24"


def test_surprise_consumers_agree_on_the_same_quarter():
    """**한 필드 안의 두 줄이 같은 분기를 가리켜야 한다.**

    `_next_earnings`(가장 나중 발표일)와 카드 표시(`surprise[:3]` 앞에서부터)는 소비자가 다른데
    정렬 계약이 갈리면 "다음 발표 = 9월(May 분기 기준)"과 "서프라이즈 첫 항목 = 1년 전 분기"가
    나란히 인쇄된다. 정렬은 `parse_surprise` 한 곳에서만 하고, 여기서 둘의 정합성을 못박는다.
    """
    payload = {  # 나스닥이 최신순을 보장하지 않는다고 가정 — 일부러 뒤섞어 넣는다
        "data": {
            "earningsSurpriseTable": {
                "rows": [
                    {
                        "fiscalQtrEnd": "May 2025",
                        "dateReported": "6/25/2025",
                        "percentageSurprise": 1,
                    },
                    {
                        "fiscalQtrEnd": "May 2026",
                        "dateReported": "6/25/2026",
                        "percentageSurprise": 2,
                    },
                    {
                        "fiscalQtrEnd": "Feb 2026",
                        "dateReported": "3/19/2026",
                        "percentageSurprise": 3,
                    },
                ]
            }
        }
    }
    rows = parse_surprise(payload)
    assert [r["fiscalQtrEnd"] for r in rows] == ["May 2026", "Feb 2026", "May 2025"]  # 최신순
    latest = rows[0]
    out = us_digest.fmt_earnings(rows, None, [], date(2026, 7, 29))
    # 다음 발표 추정의 기준 = 가장 나중 발표일 · 서프라이즈 첫 항목 = 같은 분기
    assert _next_earnings(rows, date(2026, 7, 29))[0] == "2026-09-24"
    assert f"서프라이즈 {latest['fiscalQtrEnd']}" in out


def test_next_earnings_uses_latest_row_not_first():
    # "최신이 첫 행"은 나스닥이 보장한 계약이 아니다 — 첫 행에서 즉시 return 하면 배열 순서가
    # 바뀌는 날 1년 전 날짜를 "다음 발표"로 낸다(실측 사고: D--104).
    rows = [{"dateReported": "6/25/2025"}, {"dateReported": "6/25/2026"}]
    assert _next_earnings(rows, date(2026, 7, 29))[0] == "2026-09-24"


def test_fmt_earnings_says_unknown_when_estimate_date_passed():
    # 추정일이 지났는데 이력이 안 갱신됐다 → 지난 날짜를 "다음 발표"로 내면 거짓이다.
    out = us_digest.fmt_earnings([{"dateReported": "1/15/2026"}], None, [], date(2026, 7, 29))
    assert "다음 발표 미정" in out and "경과" in out
    assert "D-" not in out.split("\n")[0]  # 음수 D-day 표기(`D--104`)가 새어나가지 않는다


def test_parse_short_interest_latest_two():
    parsed = parse_short_interest(
        {
            "data": {
                "shortInterestTable": {
                    "rows": [
                        {
                            "settlementDate": "07/15/2026",
                            "interest": "36,211,849",
                            "daysToCover": "1.23",
                        },
                        {"settlementDate": "06/30/2026", "interest": "30,000,000"},
                    ]
                }
            }
        }
    )
    assert parsed == {
        "date": "07/15/2026",
        "interest": 36211849.0,
        "days_to_cover": 1.23,
        "prior": 30000000.0,
    }


@pytest.mark.parametrize(
    "payload", [{"data": {"shortInterestTable": {"rows": []}}}, {"data": None}, None]
)
def test_parse_short_interest_empty_is_none(payload):
    assert parse_short_interest(payload) is None


def test_parse_apewisdom_finds_ticker_case_insensitive():
    payload = {"results": [{"ticker": "nv", "mentions": "9"}, {"ticker": "MU", "mentions": "142"}]}
    assert parse_apewisdom(payload, "MU")["mentions"] == "142"
    assert parse_apewisdom(payload, "AMD") is None
    assert parse_apewisdom({"results": None}, "MU") is None


def test_parse_fear_greed():
    assert parse_fear_greed({"fear_and_greed": {"score": 39.4, "rating": "fear"}})["rating"] == (
        "fear"
    )
    assert parse_fear_greed({"fear_and_greed": None}) is None
    assert parse_fear_greed(None) is None


def test_parse_news_truncates_title_and_limits():
    payload = {
        "news": [
            {"title": "가" * 120, "publisher": "R" * 50, "link": "https://x/" + "y" * 300},
            {"title": "둘", "publisher": "AFP", "link": "https://b"},
            {"title": "셋", "publisher": "CNBC", "link": "https://c"},
            {"title": "넷", "publisher": "WSJ", "link": "https://d"},
        ]
    }
    news = parse_news(payload)
    assert len(news) == 3
    assert news[0]["title"] == "가" * 90 + "…"
    assert len(news[0]["publisher"]) == 30 and len(news[0]["link"]) == 200
    assert parse_news({"news": None}) == [] and parse_news(None) == []


# ═══════════════════════════════════════════════════════════════════════════
# ⑦ 거짓 표기 방지 — `조회 실패`(못 받음) vs `없음`(그날 0건)
# ═══════════════════════════════════════════════════════════════════════════
def test_form4_none_says_failed_not_none_found():
    # 인덱스를 못 받았는데 "없음"이라고 쓰면 카드가 거짓말을 한다(§4-4·§4-6 의 핵심).
    line = _line(fmt_flows(None, None, None, None, None), "Form 4")
    assert line == f"Form 4 {FAIL}"
    assert "없음" not in line


def test_form4_empty_says_none_found_not_failed():
    line = _line(fmt_flows(None, [], None, None, None), "Form 4")
    assert line == "Form 4 없음"
    assert FAIL not in line


def test_form4_rows_show_count_owner_and_code_note():
    out = fmt_flows(
        None,
        [{"owner": "MEHROTRA SANJAY", "codes": "MS"}, {"owner": "MEHROTRA SANJAY", "codes": "MS"}],
        None,
        None,
        None,
    )
    assert _line(out, "Form 4") == "Form 4 2건 — MEHROTRA SANJAY(MS)"  # 건수 2, 표시는 중복 제거
    assert "매도 우위가 곧 악재는 아니다" in out  # 옵션행사분이 섞인다는 주석은 항상 붙는다


def test_eightk_none_says_failed_and_empty_says_none_found():
    assert _line(fmt_filings(None, []), "8-K") == f"8-K {FAIL}"
    empty = _line(fmt_filings({"day": "2026-07-28", "total": 323, "8-K": []}, []), "8-K")
    assert empty == "8-K 없음 (2026-07-28 전체 323건 중 해당 없음)"
    assert FAIL not in empty
    hit = _line(fmt_filings({"day": "2026-07-28", "total": 323, "8-K": ["p"]}, []), "8-K")
    assert hit == "8-K 1건 (2026-07-28 접수)"


def test_filings_news_failure_is_separate_from_8k():
    # 뉴스가 죽어도 "8-K 없음"은 그대로 사실이다 — 두 실패가 서로를 오염시키지 않아야 한다.
    out = fmt_filings({"day": "2026-07-28", "total": 323, "8-K": []}, [])
    assert "8-K 없음" in out and f"뉴스 {FAIL}" in out


def test_filings_rejects_url_that_could_forge_a_second_link():
    # `[제목](url)` 의 괄호를 URL 안에서 닫으면 **라벨·주소가 전부 남의 것인 링크**를 신뢰받는
    # 봇 카드에 띄울 수 있다(피싱). URL 이 거절돼도 제목·출처는 남긴다(정보를 버리지 않는다).
    news = [
        {
            "title": "정상 제목",
            "publisher": "Reuters",
            "link": "https://ok.example/a) [계정 확인 필요](https://phish.example",
        }
    ]
    out = fmt_filings({"day": "2026-07-28", "total": 1, "8-K": []}, news)
    assert "phish.example" not in out and "](" not in out
    assert "정상 제목" in out and "Reuters" in out


def test_filings_keeps_normal_link_and_escapes_title():
    news = [{"title": "MU [속보] 실적", "publisher": "P", "link": "https://ok.example/a?b=1&c=2"}]
    out = fmt_filings({"day": "2026-07-28", "total": 1, "8-K": []}, news)
    assert "(https://ok.example/a?b=1&c=2)" in out
    assert "[MU (속보) 실적]" in out  # 제목 안 대괄호는 링크를 깨뜨리므로 치환된다


# ═══════════════════════════════════════════════════════════════════════════
# ⑧ 블록 단위 부분 실패 — 죽은 소스는 그 블록만 `조회 실패`
# ═══════════════════════════════════════════════════════════════════════════
_MU_QUOTE = {
    "symbol": "MU",
    "price": 820.53,
    "prev": 900.19,
    "pct": -8.85,
    "w52h": 1213.56,
    "w52l": 61.54,
    "high": 1213.56,
    "bars": 250,
}
_FX_QUOTE = {"symbol": "KRW=X", "price": 1464.4, "prev": 1460.0, "pct": 0.3, "bars": 5}


def test_fmt_price_without_fx_marks_only_that_line():
    out = fmt_price(_MU_QUOTE, None)
    assert "$820.53" in out and "-8.85%" in out
    assert f"원화환산 {FAIL}(환율)" in out
    assert "52주" in out  # 나머지 줄은 살아 있다


def test_fmt_price_with_fx_shows_krw():
    out = fmt_price(_MU_QUOTE, _FX_QUOTE)
    assert f"₩{820.53 * 1464.4:,.0f}" in out
    assert FAIL not in out


def test_fmt_price_missing_52w_skips_that_line_only():
    out = fmt_price({**_MU_QUOTE, "w52h": None, "w52l": None}, _FX_QUOTE)
    assert "52주" not in out and "$820.53" in out and "₩" in out


def test_fmt_expectation_partial_failures():
    assert f"목표가 {FAIL}" in fmt_expectation_target_none()
    both = us_digest.fmt_expectation(None, None)
    assert f"목표가 {FAIL}" in both and f"추정치 조정 {FAIL}" in both
    assert "괴리가 벌어진 상태" in both  # 해설 주석은 실패해도 남는다


def fmt_expectation_target_none():
    return us_digest.fmt_expectation(None, parse_forecast(_FORECAST))


def test_fmt_expectation_shows_zero_adjustments_verbatim():
    # 상향 0 / 하향 0 이 최고 신호(§4-3) — 0 을 falsy 로 취급해 감추면 안 된다.
    out = us_digest.fmt_expectation(parse_targetprice(_TARGETPRICE), parse_forecast(_FORECAST))
    assert "최근 4주 추정치 상향 0 · 하향 0" in out
    assert "2026-06 $1,036 → 2026-07 $1,569" in out


def test_fmt_earnings_all_sources_dead():
    out = us_digest.fmt_earnings([], None, [], date(2026, 7, 29))
    assert f"다음 발표일 {FAIL}" in out and f"서프라이즈 {FAIL}" in out


def test_fmt_fundamentals_no_facts():
    field, warn = fmt_fundamentals(None, 820.53, None, None)
    assert field == f"SEC 재무 {FAIL}" and warn == ""


def test_fmt_fundamentals_mcap_crosscheck_warns_only_beyond_tolerance():
    facts = {
        "quarters": [
            {"end": "2025-05-31", "rev": 1e10, "gross": 5e9, "op": 3e9, "eps": 5.0, "inv": 8e9},
            {"end": "2025-08-31", "rev": 1.2e10, "gross": 6e9, "op": 4e9, "eps": 6.0, "inv": 9e9},
        ],
        "shares": 1_000_000_000,
    }
    ok_field, ok_warn = fmt_fundamentals(facts, 100.0, None, 100e9)  # SEC 100B vs Nasdaq 100B
    assert ok_warn == "" and "시총 SEC 100.0B" in ok_field
    _bad_field, bad_warn = fmt_fundamentals(facts, 100.0, None, 50e9)  # 2배 차이
    assert "시총 교차검증 불일치" in bad_warn


def test_fmt_fundamentals_ttm_failed_when_quarters_short():
    facts = {"quarters": [{"end": "2025-08-31", "rev": 1e10, "eps": 6.0}], "shares": 0}
    field, _warn = fmt_fundamentals(facts, 100.0, None, None)
    assert f"TTM {FAIL}" in field  # 1분기뿐이면 TTM 을 지어내지 않는다
    assert "최근분기 연율" in field


def test_fmt_korea_all_dead():
    out = fmt_korea({}, None)
    assert out.count(FAIL) == 3  # 삼성·SK하이닉스(코스피)·SKHY
    assert "한국장이 미장보다 먼저 열린다" in out


def test_fmt_korea_skhy_marks_ipo_age_and_skips_period_compare():
    quotes = {
        "005930.KS": {"price": 84000.0, "pct": -5.68},
        "000660.KS": {"price": 512000.0, "pct": -10.52},
        "SKHY": {"price": 41.2, "pct": -8.98, "bars": 13, "high": 55.0},
    }
    out = fmt_korea(quotes, None)
    assert "[상장 13일차]" in out
    assert "고점 대비 -25.1%" in out


def test_fmt_korea_skhy_no_ipo_tag_after_enough_bars():
    quotes = {"SKHY": {"price": 41.2, "pct": -1.0, "bars": 90, "high": 55.0}}
    assert "[상장" not in fmt_korea(quotes, None)


def test_fmt_korea_skhy_forecast_always_states_estimate_count():
    # 추정인원 1~2명을 MU(12명)와 같은 무게로 읽으면 안 된다 → 인원 병기가 사라지면 안 됨.
    forecast = {"year": {"consensusEPSForecast": "2.10", "fiscalEnd": "Dec", "noOfEstimates": 2}}
    out = fmt_korea({}, forecast)
    assert "추정 2인" in out and "대표성은 낮다" in out


def test_fmt_sector_partial_quotes():
    out = fmt_sector({"^SOX": {"pct": -3.2}, "NVDA": {"pct": -2.1}})
    assert "SOX -3.2%" in out and f"SMH {FAIL}" in out and "NVDA -2.1%" in out
    assert "AMD" not in out  # 죽은 종목은 목록에서 빠지되(한 줄이 실패로 도배되지 않게)
    assert f"(8종 {FAIL})" in out  # 몇 종이 빠졌는지는 꼬리로 남는다


def test_fmt_sector_all_dead():
    # 9종이 통째로 죽어도 **몇 종이 빠졌는지**는 남아야 한다(빈 줄이면 "오늘은 이게 다"로 읽힌다).
    assert f"(9종 {FAIL})" in fmt_sector({})


def test_fmt_flows_all_dead():
    out = fmt_flows(None, None, None, None, None)
    for prefix in ("공매도", "Form 4", "레딧 언급", "심리지표"):
        assert f"{prefix} {FAIL}" in out or _line(out, prefix).endswith(FAIL)


def test_fmt_flows_fear_greed_omits_missing_previous_close():
    # 공포탐욕에서 **0 은 결측이 아니라 "극단적 공포"라는 실값**이다 → `or 0` 으로 채우면
    # 하루 만에 극단공포→중립으로 튄 것처럼 읽힌다. 없으면 아예 안 적는다.
    out = fmt_flows(None, None, None, {"score": 40.0, "rating": "fear"}, None)
    assert "공포탐욕 40 fear" in out and "전일" not in out
    zero = fmt_flows(None, None, None, {"score": 40.0, "rating": "f", "previous_close": 0}, None)
    assert "(전일 0)" in zero  # 진짜 0 은 표기한다


def test_fmt_flows_form4_shows_index_day():
    # 인덱스가 하루 이상 거슬러 올라갔을 때 이틀 전 내부자거래가 오늘 것처럼 보이면 안 된다.
    out = fmt_flows(None, [{"owner": "A", "codes": "S"}], None, None, None, "2026-07-28")
    assert "Form 4 1건 (2026-07-28)" in out


def test_fmt_flows_missing_reddit_keys_do_not_print_none():
    # 신규 추적 티커는 `mentions_24h_ago`·`rank` 가 비어 온다 — `str(None)` 이 그대로 찍히면
    # 카드에 `(전일 None · 전체 None위)` 가 나간다.
    out = fmt_flows(None, None, {"ticker": "MU", "mentions": 5}, None, None)
    assert "None" not in out


def test_plain_blocks_spoiler_bars():
    # `||…||` 는 그 사이를 **가린다**. Form 4 의 rptOwnerName 은 제출자가 통제하는 값이라
    # 이름 사이에 `||` 를 심으면 `※ S=매도 …` 해석 가드가 숨겨진다.
    out = fmt_flows(
        None, [{"owner": "A||", "codes": "S"}, {"owner": "||B", "codes": "S"}], None, None, None
    )
    assert "||" not in out
    assert "매도 우위가 곧 악재는 아니다" in out


def test_fmt_flows_escapes_markdown_from_external_names():
    # 보고자·레딧·CNN 문자열은 전부 외부 값 — 링크 문법을 심을 수 있다.
    out = fmt_flows(
        None,
        [{"owner": "[계정 확인](https://phish.example)", "codes": "S"}],
        {"ticker": "MU", "mentions": 1, "mentions_24h_ago": 1, "rank": 1},
        None,
        None,
    )
    assert "](" not in out


# ═══════════════════════════════════════════════════════════════════════════
# ⑨ 통합 — 네트워크 seam(`_get`)만 갈아끼운 카드 조립
# ═══════════════════════════════════════════════════════════════════════════
_SEC_FACTS = {
    "cik": 723125,
    "entityName": "MICRON TECHNOLOGY, INC.",
    "facts": {
        "dei": {
            "EntityCommonStockSharesOutstanding": {
                "units": {
                    "shares": [
                        {"end": "2026-06-25", "val": 1_130_000_000, "form": "10-Q"},
                    ]
                }
            }
        },
        "us-gaap": {
            "RevenueFromContractWithCustomerExcludingAssessedTax": {
                "units": {
                    "USD": [
                        _row("2025-09-01", "2025-11-27", 1.2e10, "2025-12-20"),
                        _row("2025-11-28", "2026-02-26", 1.4e10, "2026-03-20"),
                        _row("2026-02-27", "2026-05-28", 1.6e10, "2026-06-20"),
                        _row("2026-05-29", "2026-08-27", 1.8e10, "2026-09-20"),
                    ]
                }
            },
            "GrossProfit": {
                "units": {
                    "USD": [
                        _row("2026-02-27", "2026-05-28", 9.0e9, "2026-06-20"),
                        _row("2026-05-29", "2026-08-27", 1.05e10, "2026-09-20"),
                    ]
                }
            },
            "OperatingIncomeLoss": {
                "units": {
                    "USD": [
                        _row("2026-02-27", "2026-05-28", 7.0e9, "2026-06-20"),
                        _row("2026-05-29", "2026-08-27", 8.2e9, "2026-09-20"),
                    ]
                }
            },
            "NetIncomeLoss": {
                "units": {"USD": [_row("2026-05-29", "2026-08-27", 6.4e9, "2026-09-20")]}
            },
            "EarningsPerShareDiluted": {
                "units": {
                    "USD/shares": [
                        _row("2025-09-01", "2025-11-27", 3.1, "2025-12-20"),
                        _row("2025-11-28", "2026-02-26", 3.9, "2026-03-20"),
                        _row("2026-02-27", "2026-05-28", 4.8, "2026-06-20"),
                        _row("2026-05-29", "2026-08-27", 5.6, "2026-09-20"),
                    ]
                }
            },
            "InventoryNet": {
                "units": {
                    "USD": [
                        {"end": "2026-05-28", "val": 9.1e9, "filed": "2026-06-20"},
                        {"end": "2026-08-27", "val": 9.6e9, "filed": "2026-09-20"},
                    ]
                }
            },
        },
    },
}

_FORM4_XML = (
    '<?xml version="1.0"?><ownershipDocument>'
    "<reportingOwner><reportingOwnerId>"
    "<rptOwnerName>MEHROTRA SANJAY</rptOwnerName></reportingOwnerId></reportingOwner>"
    "<nonDerivativeTable><nonDerivativeTransaction>"
    "<transactionCoding><transactionCode>S</transactionCode></transactionCoding>"
    "</nonDerivativeTransaction><nonDerivativeTransaction>"
    "<transactionCoding><transactionCode>M</transactionCode></transactionCoding>"
    "</nonDerivativeTransaction></nonDerivativeTable></ownershipDocument>"
)

_LIVE_IDX = _idx(
    _idx_row("4", "MEHROTRA SANJAY", "1234567", "edgar/data/723125/f4a.txt"),
    _idx_row("4", "MURPHY MARK J", "7654321", "edgar/data/723125/f4b.txt"),
    _idx_row("8-K", "OTHER CORP", "999999", "edgar/data/999999/other.txt"),
)


def _routes():
    """(host, path 부분문자열) → 응답. 앞에서부터 처음 맞는 것을 쓴다(구체적인 것을 먼저)."""
    return [
        (
            "query1.finance.yahoo.com",
            "/v1/finance/search",
            {
                "news": [
                    {
                        "title": "Micron slides as AI chip rally cools",
                        "publisher": "Reuters",
                        "link": "https://example.com/a",
                    },
                    {
                        "title": "SK hynix Q2 profit up 1,200%",
                        "publisher": "AFP",
                        "link": "https://example.com/b",
                    },
                ]
            },
        ),
        (
            "query1.finance.yahoo.com",
            "chart/MU?",
            _chart([900.19, 820.53], fiftyTwoWeekHigh=1213.56, fiftyTwoWeekLow=61.54, symbol="MU"),
        ),
        ("query1.finance.yahoo.com", "chart/SKHY?", _chart([45.0, 41.2], symbol="SKHY")),
        ("query1.finance.yahoo.com", "chart/KRW%3DX?", _chart([1460.0, 1464.4], symbol="KRW=X")),
        ("query1.finance.yahoo.com", "chart/", _chart([100.0, 98.0])),  # 나머지 심볼 공통
        ("api.nasdaq.com", "/targetprice", _TARGETPRICE),
        (
            "api.nasdaq.com",
            f"/api/analyst/{us_digest.SKHY}/earnings-forecast",
            {
                "data": {
                    "yearlyForecast": {
                        "rows": [
                            {
                                "fiscalEnd": "Dec",
                                "consensusEPSForecast": "2.10",
                                "noOfEstimates": "2",
                            }
                        ]
                    }
                }
            },
        ),
        ("api.nasdaq.com", "/earnings-forecast", _FORECAST),
        (
            "api.nasdaq.com",
            "/earnings-surprise",
            {
                "data": {
                    "earningsSurpriseTable": {
                        "rows": [
                            {
                                "fiscalQtrEnd": "May2026",
                                "dateReported": "6/25/2026",
                                "percentageSurprise": "12.34",
                            },
                            {
                                "fiscalQtrEnd": "Feb2026",
                                "dateReported": "3/20/2026",
                                "percentageSurprise": "8.10",
                            },
                        ]
                    }
                }
            },
        ),
        (
            "api.nasdaq.com",
            "/short-interest",
            {
                "data": {
                    "shortInterestTable": {
                        "rows": [
                            {
                                "settlementDate": "07/15/2026",
                                "interest": "36,211,849",
                                "daysToCover": "1.23",
                            },
                            {"settlementDate": "06/30/2026", "interest": "30,000,000"},
                        ]
                    }
                }
            },
        ),
        (
            "api.nasdaq.com",
            "/summary",
            {
                "data": {
                    "summaryData": {
                        "MarketCap": {"label": "Market Cap", "value": "926,700,000,000"}
                    }
                }
            },
        ),
        (
            "api.nasdaq.com",
            "/calendar/earnings",
            {
                "data": {
                    "rows": [{"symbol": "NVDA", "time": "time-after-hours", "epsForecast": "$1.20"}]
                }
            },
        ),
        ("data.sec.gov", "/api/xbrl/companyfacts/", _SEC_FACTS),
        ("www.sec.gov", "/daily-index/", _LIVE_IDX.encode()),
        ("www.sec.gov", "/Archives/edgar/data/723125/f4a.txt", _FORM4_XML.encode()),
        (
            "www.sec.gov",
            "/Archives/edgar/data/723125/f4b.txt",
            _FORM4_XML.replace("MEHROTRA SANJAY", "MURPHY MARK J").encode(),
        ),
        (
            "apewisdom.io",
            "/api/v1.0/filter/",
            {
                "results": [
                    {"ticker": "MU", "mentions": "142", "mentions_24h_ago": "98", "rank": "3"}
                ]
            },
        ),
        (
            "production.dataviz.cnn.io",
            "/index/fearandgreed/",
            {"fear_and_greed": {"score": 39.4, "rating": "fear", "previous_close": 42.1}},
        ),
    ]


class _FakeNet:
    """`us_digest._get` 대체 — `_get` 의 3값 계약을 그대로 흉내낸다.

    라우트에 있으면 본문 · **없으면 `b""`(서버가 "그런 건 없다"고 답함)** · `drop` 이면 `None`
    (조회 자체 실패 = 있는지 없는지 모름). 이 둘을 안 가르면 "없는 날"과 "타임아웃"이 같아져
    fetch_daily_index 가 멀쩡한 날을 건너뛴다.
    """

    def __init__(self, routes, drop=()):
        self.routes = routes
        self.drop = tuple(drop)  # 이 조각이 path 에 있으면 강제로 None(소스 장애 시뮬레이션)
        self.calls: list[tuple[str, str]] = []

    def __call__(self, host, path, _headers=None):
        self.calls.append((host, path))
        assert host in us_digest._HOSTS, f"allowlist 밖 host: {host}"
        if any(frag in path for frag in self.drop):
            return None
        for route_host, frag, body in self.routes:
            if route_host == host and frag in path:
                return body if isinstance(body, bytes) else json.dumps(body).encode()
        return b""


@pytest.fixture
def net(monkeypatch):
    """네트워크 seam 을 통째로 가짜로. 반환값을 통해 drop 을 조정한다."""
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    fake = _FakeNet(_routes())
    monkeypatch.setattr(us_digest, "_get", fake)
    return fake


@pytest.mark.usefixtures("net")
def test_build_us_digest_full_card():
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    assert spec["title"] == "📈 미국주식 2026-07-29 · MU $820.53 -8.85%"
    assert spec["color"] == us_digest.COLOR_DOWN
    names = [n for n, _v, _i in spec["fields"]]
    assert names == [
        "💵 MU 시세",
        "🎯 시장 기대",
        "📅 실적",
        "🏭 펀더멘털(SEC)",
        "🔄 수급·심리",
        "📰 공시·뉴스",
        "🇰🇷 한국 메모리 3사",
        "🧠 섹터",
    ]
    values = {n: v for n, v, _i in spec["fields"]}
    assert "₩" in values["💵 MU 시세"]  # 원화환산(§4-1)
    assert "상향 0 · 하향 0" in values["🎯 시장 기대"]  # 0 건도 표기(§4-3)
    assert "Form 4 2건" in values["🔄 수급·심리"]
    assert "8-K 없음" in values["📰 공시·뉴스"]  # MU 8-K 는 그날 없었다 → 그 자체가 정보(§4-6)
    assert "TTM" in values["🏭 펀더멘털(SEC)"] and FAIL not in values["🏭 펀더멘털(SEC)"]
    assert "판단 재료 제공(투자 조언 아님)" in spec["footer"]


def test_build_us_digest_no_network_calls_outside_fake(net):
    build_us_digest("2026-07-29")
    assert net.calls, "네트워크 seam 이 안 불렸다 — 테스트가 공허하다"
    assert {h for h, _p in net.calls} <= us_digest._HOSTS


def test_build_us_digest_returns_none_when_mu_quote_dead(net):
    # 보유 종목 시세가 없으면 카드를 내지 않는다 → 호출측이 fired 를 되돌려 다음 틱에 재시도.
    net.drop = ("chart/MU?",)
    assert build_us_digest("2026-07-29") is None


def test_build_us_digest_survives_everything_else_dead(monkeypatch):
    """MU 시세 하나만 살아 있으면 카드는 나간다 — 나머지는 전부 `조회 실패` 블록."""
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    only_mu = _FakeNet([r for r in _routes() if r[1] == "chart/MU?"])
    monkeypatch.setattr(us_digest, "_get", only_mu)
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    values = {n: v for n, v, _i in spec["fields"]}
    assert len(values) == 8
    assert FAIL not in values["💵 MU 시세"].split("\n")[0]  # 시세 본줄은 살아 있다
    for name in ("🎯 시장 기대", "🏭 펀더멘털(SEC)", "📰 공시·뉴스", "🧠 섹터"):
        assert FAIL in values[name], name


@pytest.mark.parametrize(
    ("blockname", "field"),
    [
        ("fmt_sector", "🧠 섹터"),
        ("fmt_korea", "🇰🇷 한국 메모리 3사"),
        ("fmt_flows", "🔄 수급·심리"),
        ("fmt_filings", "📰 공시·뉴스"),
        ("fmt_expectation", "🎯 시장 기대"),
        ("fmt_earnings", "📅 실적"),
        ("fmt_price", "💵 MU 시세"),
    ],
)
@pytest.mark.usefixtures("net")
def test_formatter_exception_degrades_only_its_block(monkeypatch, blockname, field):
    """포매터가 터져도(상류가 예상 못 한 타입을 보냄) 그 블록만 `조회 실패` — 카드는 나간다."""

    def boom(*_a, **_k):
        raise TypeError("예상 못 한 타입")

    monkeypatch.setattr(us_digest, blockname, boom)
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    values = {n: v for n, v, _i in spec["fields"]}
    assert values[field] == FAIL
    assert len([v for v in values.values() if v == FAIL]) == 1  # 나머지 7블록은 멀쩡


@pytest.mark.usefixtures("net")
def test_fundamentals_exception_degrades_only_its_block(monkeypatch):
    # 이 블록만 반환이 튜플이라 _safe 를 못 쓴다 → 별도 try 가 같은 태도로 감싸는지 확인.
    def boom(*_a, **_k):
        raise ZeroDivisionError

    monkeypatch.setattr(us_digest, "fmt_fundamentals", boom)
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    values = {n: v for n, v, _i in spec["fields"]}
    assert values["🏭 펀더멘털(SEC)"] == f"SEC 재무 {FAIL}"
    assert "⚠️" not in spec["footer"]  # 경고도 함께 비워진다(깨진 계산으로 경고를 내지 않는다)


@pytest.mark.parametrize(
    ("drop", "field", "must_fail"),
    [
        (("targetprice",), "🎯 시장 기대", "목표가"),
        (("companyfacts",), "🏭 펀더멘털(SEC)", "SEC 재무"),
        (("daily-index",), "📰 공시·뉴스", "8-K"),
        (("short-interest",), "🔄 수급·심리", "공매도"),
        (("/api/v1.0/filter/",), "🔄 수급·심리", "레딧 언급"),
        (("KRW%3DX",), "💵 MU 시세", "원화환산"),
    ],
)
def test_single_source_failure_degrades_only_its_block(net, drop, field, must_fail):
    net.drop = drop
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    values = {n: v for n, v, _i in spec["fields"]}
    assert FAIL in _line(values[field], must_fail) or f"{must_fail} {FAIL}" in values[field]
    # 다른 블록은 멀쩡해야 한다(한 소스 장애가 카드 전체를 실패로 물들이지 않게).
    assert FAIL not in values["🇰🇷 한국 메모리 3사"]


def test_form4_count_survives_document_fetch_failure(net):
    # 인덱스가 2건이라고 했는데 원문 1건만 받아졌다 → 건수는 2 유지 + 부족분 `?`.
    net.drop = ("f4b.txt",)
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    flows = {n: v for n, v, _i in spec["fields"]}["🔄 수급·심리"]
    assert "Form 4 2건" in flows and "?(?)" in flows


def test_daily_index_walks_back_to_previous_business_day(monkeypatch):
    # 주말·휴일이면 전일 인덱스가 없다 → 최대 4일 거슬러 첫 성공에서 멈춘다.
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    fake = _FakeNet([("www.sec.gov", "form.20260731.idx", _LIVE_IDX.encode())])
    monkeypatch.setattr(us_digest, "_get", fake)
    found = us_digest.fetch_daily_index("2026-08-03", "723125")  # 월요일 → 금요일까지 역행
    assert found is not None and found["day"] == "2026-07-31"
    assert len(found["4"]) == 2


@pytest.mark.parametrize(
    "rel",
    [
        "edgar/data/999999/x.txt",  # 남의 폴더
        "edgar/data/723125/../../../999999/x.txt",  # `..` 우회 — urllib 은 정규화 없이 보내고
        "edgar/data/723125/..%2f..%2fx.txt",  # S3 가 정규화한다(실측) → startswith 로는 못 막는다
        "/etc/passwd",
    ],
)
def test_fetch_form4_details_rejects_paths_outside_the_ticker_folder(monkeypatch, rel):
    # 이 가드가 막으려는 건 **타사 내부자 이름이 MU 내부자거래로 카드에 실리는 것**이다.
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    monkeypatch.setattr(
        us_digest, "_get", lambda *_a, **_k: pytest.fail(f"경로 이탈 요청이 나갔다: {rel}")
    )
    assert us_digest.fetch_form4_details([rel]) == []


def test_daily_index_treats_html_body_as_failure_not_empty_day(monkeypatch):
    """SEC 가 200 으로 form.idx 가 아닌 본문(점검·오류 HTML)을 주면 데이터줄이 0 이 된다.

    그대로 흘리면 카드에 `8-K 없음 (전체 0건 중 해당 없음)` 이 실린다 — **미국 전체 공시가 0건인
    날은 없으므로** 그건 없는 사실이다. 파서 단위 테스트만 있으면 이 계층을 못 잡는다.
    """
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    html = b"<html><body>Service temporarily unavailable</body></html>"
    monkeypatch.setattr(us_digest, "_get", lambda *_a, **_k: html)
    assert us_digest.fetch_daily_index("2026-07-29", "723125") is None


def test_daily_index_none_after_four_days(monkeypatch):
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    monkeypatch.setattr(us_digest, "_get", _FakeNet([]))
    assert us_digest.fetch_daily_index("2026-08-03", "723125") is None


def test_sec_blocks_skipped_without_user_agent(monkeypatch):
    # `.env` 에 SEC_USER_AGENT 가 없으면 SEC 는 403 이므로 아예 안 부른다(그 블록만 실패).
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "")
    fake = _FakeNet(_routes())
    monkeypatch.setattr(us_digest, "_get", fake)
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    assert not [p for h, p in fake.calls if h in ("www.sec.gov", "data.sec.gov")]
    values = {n: v for n, v, _i in spec["fields"]}
    assert f"SEC 재무 {FAIL}" in values["🏭 펀더멘털(SEC)"]
    assert f"8-K {FAIL}" in values["📰 공시·뉴스"]
    assert f"Form 4 {FAIL}" in values["🔄 수급·심리"]  # 못 받은 것이지 "없음"이 아니다


def test_sec_facts_cached_per_day(monkeypatch, tmp_path):
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    monkeypatch.setattr(us_digest, "SEC_CACHE_FILE", tmp_path / "cache.json")
    fake = _FakeNet(_routes())
    monkeypatch.setattr(us_digest, "_get", fake)
    first = us_digest.fetch_sec_facts("2026-07-29")
    second = us_digest.fetch_sec_facts("2026-07-29")
    assert first == second
    hits = [p for h, p in fake.calls if h == "data.sec.gov"]
    assert len(hits) == 1  # 4MB 원본은 하루 1회만
    us_digest.fetch_sec_facts("2026-07-30")  # 날짜가 바뀌면 다시 받는다
    assert len([p for h, p in fake.calls if h == "data.sec.gov"]) == 2


def test_parse_sec_facts_takes_last_four_quarters():
    facts = parse_sec_facts(_SEC_FACTS)
    assert facts is not None
    assert [q["end"] for q in facts["quarters"]] == [
        "2025-11-27",
        "2026-02-26",
        "2026-05-28",
        "2026-08-27",
    ]
    assert facts["shares"] == 1_130_000_000
    assert facts["quarters"][-1]["inv"] == 9.6e9
    assert facts["quarters"][0]["gross"] is None  # 없는 분기는 None(0 으로 채우지 않는다)


@pytest.mark.parametrize(
    "payload",
    [None, {}, {"facts": None}, {"facts": {"us-gaap": None}}, {"facts": {"us-gaap": {}}}],
)
def test_parse_sec_facts_malformed_is_none(payload):
    assert parse_sec_facts(payload) is None


def test_parse_sec_facts_falls_back_to_revenues_tag():
    payload = {"facts": {"us-gaap": _gaap([_row("2026-05-29", "2026-08-27", 5.0, "2026-09-20")])}}
    facts = parse_sec_facts(payload)
    assert facts is not None and facts["quarters"][0]["rev"] == 5.0


# ═══════════════════════════════════════════════════════════════════════════
# ⑩ 디스코드 한도 — field 1024 · embed 총합 6000
# ═══════════════════════════════════════════════════════════════════════════
def _embed_total(spec):
    """디스코드가 6000 으로 세는 것 = title + 모든 field name/value + footer."""
    total = len(spec.get("title") or "") + len(spec.get("footer") or "")
    for name, value, _inline in spec["fields"]:
        total += len(name) + len(value)
    return total


@pytest.mark.usefixtures("net")
def test_card_fits_discord_limits():
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    for name, value, _inline in spec["fields"]:
        assert len(value) <= DISCORD_FIELD_MAX, f"{name} 필드 {len(value)}자"
        assert len(value) <= us_digest.FIELD_MAXLEN
    assert _embed_total(spec) <= DISCORD_EMBED_TOTAL_MAX


def test_field_budget_product_stays_under_embed_total():
    # FIELD_MAXLEN 을 키우거나 필드를 늘릴 때 이 곱을 다시 재라(모듈 상수 주석의 계약).
    fields = 8
    assert fields * us_digest.FIELD_MAXLEN + 200 <= DISCORD_EMBED_TOTAL_MAX


def test_card_fits_limits_even_with_absurd_upstream_values(monkeypatch):
    """상류가 수십 KB 짜리 문자열을 보내도 필드 한도를 넘지 않는다(계약 이탈 방어)."""
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    routes = _routes()
    routes.insert(
        0,
        (
            "query1.finance.yahoo.com",
            "/v1/finance/search",
            {
                "news": [
                    {
                        "title": "가" * 5000,
                        "publisher": "나" * 5000,
                        "link": "https://x/" + "y" * 5000,
                    }
                ]
                * 5
            },
        ),
    )
    routes.insert(
        0,
        (
            "api.nasdaq.com",
            "/targetprice",
            {
                "data": {
                    "consensusOverview": {
                        "priceTarget": 1.0,
                        "buy": "다" * 3000,
                        "hold": 1,
                        "sell": 0,
                    },
                    "historicalConsensus": [
                        {"y": str(i), "z": {"date": f"{(i % 12) + 1:02d}/01/2026"}}
                        for i in range(200)
                    ],
                }
            },
        ),
    )
    monkeypatch.setattr(us_digest, "_get", _FakeNet(routes))
    spec = build_us_digest("2026-07-29")
    assert spec is not None
    for name, value, _inline in spec["fields"]:
        assert len(value) <= DISCORD_FIELD_MAX, f"{name} 필드 {len(value)}자"
    assert _embed_total(spec) <= DISCORD_EMBED_TOTAL_MAX


@pytest.mark.parametrize(
    "junk",
    [
        {"data": None},  # falsy — `or {}` 로도 막히던 옛 케이스
        {"data": [{"summaryData": "x"}]},  # **truthy 쓰레기** — `or {}` 는 이걸 통과시킨다
        {"data": {"summaryData": [1]}},
        {"data": {"summaryData": {"MarketCap": "912B"}}},  # 셀이 dict 가 아님
        "not a dict",
        None,
    ],
)
def test_parse_summary_mcap_survives_truthy_garbage(junk):
    # 시총은 카드 조립 본문에서 계산돼 **어느 블록 try 에도 없다** → 여기서 터지면 MU 시세가
    # 멀쩡한데도 카드 전체가 사라진다(교차검증 한 줄 때문에).
    assert us_digest.parse_summary_mcap(junk) is None


def test_parse_summary_mcap_reads_value():
    payload = {"data": {"summaryData": {"MarketCap": {"value": "912,662,605,323"}}}}
    assert us_digest.parse_summary_mcap(payload) == 912662605323.0


def test_build_us_digest_never_raises_on_garbage_payloads(monkeypatch):
    """모든 엔드포인트가 형태가 다른 쓰레기를 뱉어도 예외 없이 카드 또는 None 이 나온다."""
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")
    garbage = [
        # `data` 는 **truthy 쓰레기**로 둔다 — falsy(None)만 넣으면 `or {}` 류 방어가 통과해
        # 실제 결함(AttributeError 로 카드 전체 소실)을 못 잡는다.
        (host, "", {"data": [1], "results": "x", "news": 1, "fear_and_greed": []})
        for host in us_digest._HOSTS
        if host != "query1.finance.yahoo.com"
    ]
    garbage.append(("query1.finance.yahoo.com", "chart/", _chart([1.0, 2.0])))
    garbage.append(("query1.finance.yahoo.com", "/v1/finance/search", {"news": "nope"}))
    monkeypatch.setattr(us_digest, "_get", _FakeNet(garbage))
    spec = build_us_digest("2026-07-29")
    assert spec is not None and len(spec["fields"]) == 8


def test_json_returns_none_for_non_json_body(monkeypatch):
    # SEC·Nasdaq 은 차단 시 JSON 이 아니라 HTML 오류 페이지를 200 으로 준다 → 파싱 예외 금지.
    monkeypatch.setattr(us_digest, "_get", lambda *_a, **_k: b"<html>Access Denied</html>")
    assert us_digest._json("api.nasdaq.com", "/x") is None
    monkeypatch.setattr(us_digest, "_get", lambda *_a, **_k: None)
    assert us_digest._json("api.nasdaq.com", "/x") is None


def test_get_swallows_network_errors(monkeypatch):
    def boom(*_a, **_k):
        raise TimeoutError("타임아웃")

    monkeypatch.setattr(us_digest._NOREDIRECT_OPENER, "open", boom)
    # 타임아웃은 **`None`(모름)** 이다 — `b""`(없음)로 흘리면 호출측이 "그날 공시 0건"으로 읽는다.
    assert us_digest._get("api.nasdaq.com", "/x") is None


@pytest.mark.parametrize(
    ("code", "headers", "want"),
    [
        (404, {}, b""),  # 서버가 "그런 건 없다"
        # 403 은 **두 가지**다(2026-07-29 실측). 파일 없음(주말·오타)은 S3 가 내고 응답에
        # x-amz-request-id 가 붙는다(Server: Apache · application/xml).
        (403, {"x-amz-request-id": "1GJ688DJMV0W95V1", "Content-Type": "application/xml"}, b""),
        # 차단(UA 거부·레이트리밋)은 Akamai WAF 가 낸다(text/html · amz 헤더 없음).
        # 이걸 "없음"으로 흘리면 레이트리밋 하루치가 "그날 공시 0건"으로 단언된다.
        (403, {"Content-Type": "text/html", "Server": "AkamaiGHost"}, None),
        (403, {}, None),  # 판단 근거가 없으면 "없음"이라 단정하지 않는다
        (302, {}, None),  # 리다이렉트 미추종 → 승격된 HTTPError. "없음"이 아니다
        (500, {}, None),  # 서버 장애 — 있는지 없는지 모른다
    ],
)
def test_get_separates_absent_from_blocked(monkeypatch, code, headers, want):
    def raise_http(*_a, **_k):
        raise urllib.error.HTTPError("https://x/y", code, "e", headers, None)

    monkeypatch.setattr(us_digest._NOREDIRECT_OPENER, "open", raise_http)
    assert us_digest._get("www.sec.gov", "/x") == want


def test_daily_index_rate_limited_403_does_not_claim_empty_day(monkeypatch):
    """레이트리밋(WAF 403)이 전일에만 걸리면 **역행해서 엉뚱한 날짜로 `8-K 없음` 을 단언**하던
    자리. 차단 403 은 `None`(모름)이므로 역행 없이 조회 실패로 끝나야 한다."""
    monkeypatch.setattr(us_digest, "_sec_ua", lambda: "tester tester@example.com")

    def blocked(*_a, **_k):
        raise urllib.error.HTTPError("https://x/y", 403, "e", {"Server": "AkamaiGHost"}, None)

    monkeypatch.setattr(us_digest._NOREDIRECT_OPENER, "open", blocked)
    assert us_digest.fetch_daily_index("2026-07-29", "723125") is None


def test_get_does_not_follow_redirects():
    """3xx 를 추종하면 ① host allowlist 밖으로 나가고(그 고정이 SSRF 방어의 근거다) ② urllib 이
    리다이렉트 때 헤더를 재전송해 **SEC UA(연락처 이메일)까지 딸려 간다**.

    `bridge._digest_get` 과 같은 opener 를 쓰는지 + 그 opener 가 3xx 를 실제로 거절하는지를 본다
    (`redirect_request` 가 None 이면 urllib 이 HTTPError 로 승격 → 위 테이블에서 `None`).
    """
    handlers = [h for h in us_digest._NOREDIRECT_OPENER.handlers if hasattr(h, "redirect_request")]
    assert handlers, "리다이렉트 핸들러가 없다"
    assert all(
        h.redirect_request(None, None, 302, "Found", {}, "https://evil.example") is None
        for h in handlers
    )


@pytest.mark.parametrize(
    ("content", "want"),
    [
        # ⚠️ 한글 UA 는 **쓰면 안 된다** — HTTP 헤더는 latin-1 이라 putheader 가
        # UnicodeEncodeError 를 내고 `_get` 이 그걸 삼켜 SEC 블록 3개가 매일 조용히 죽는다.
        # 여기서 ""(미설정)로 떨어뜨려 경고를 남긴다.
        ('SEC_USER_AGENT="홍길동 me@example.com"\n', ""),
        ("SEC_USER_AGENT=yeo junggi me@example.com\n", "yeo junggi me@example.com"),
        ("SEC_USER_AGENT=me@example.com\n", "me@example.com"),
        ("OTHER=1\nSEC_USER_AGENT = spaced@example.com \n", "spaced@example.com"),
        ("OTHER=1\n", ""),  # 미설정 → SEC 블록만 건너뜀
        ("", ""),
    ],
)
def test_sec_ua_reads_env_file(monkeypatch, tmp_path, content, want):
    env = tmp_path / ".env"
    env.write_text(content, encoding="utf-8")
    monkeypatch.setattr(us_digest, "_ENV_FILE", env)
    assert us_digest._sec_ua() == want


def test_sec_ua_missing_env_file(monkeypatch, tmp_path):
    monkeypatch.setattr(us_digest, "_ENV_FILE", tmp_path / "nope.env")
    assert us_digest._sec_ua() == ""


def test_series_parsers_skip_rows_missing_required_keys():
    assert _duration_series(_gaap([{"val": 1.0, "filed": "2025-01-01"}]), "Revenues") == {}
    assert _duration_series(_gaap([{"start": "2025-01-01", "end": "2025-03-31"}]), "Revenues") == {}
    inv = _gaap([{"end": 20250531, "val": 1.0}, {"end": "2025-05-31", "val": None}], "InventoryNet")
    assert _instant_series(inv, "InventoryNet") == {}


def test_parse_news_skips_non_dict_rows():
    assert parse_news({"news": ["junk", 3, {"title": "T", "publisher": "P", "link": "L"}]}) == [
        {"title": "T", "publisher": "P", "link": "L"}
    ]


def test_get_caps_body_and_sends_required_headers(monkeypatch):
    """정상 응답 경로 — 본문은 _MAXBYTES 로 잘리고, 지정 헤더가 그대로 요청에 실린다.

    SEC 는 UA 에 이메일이 없으면 403 이고, CNN 은 Referer·Origin 이 없으면 HTTPError 다(§1-1) →
    호출측이 준 헤더가 소실되면 그 소스가 통째로 죽는다.
    """
    seen = {}

    class _Resp:
        def read(self, size):
            seen["size"] = size
            return b"x" * 10

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    def fake_urlopen(req, timeout=None):
        seen["url"] = req.full_url
        seen["headers"] = dict(req.header_items())
        seen["timeout"] = timeout
        return _Resp()

    monkeypatch.setattr(us_digest._NOREDIRECT_OPENER, "open", fake_urlopen)
    body = us_digest._get("production.dataviz.cnn.io", "/index/x", us_digest._CNN_HEADERS)
    assert body == b"x" * 10
    assert seen["size"] == us_digest._MAXBYTES and seen["timeout"] == us_digest._TIMEOUT
    assert seen["url"] == "https://production.dataviz.cnn.io/index/x"
    lowered = {k.lower(): v for k, v in seen["headers"].items()}
    assert lowered["referer"] == "https://edition.cnn.com/"
    assert lowered["origin"] == "https://edition.cnn.com"
    assert "Mozilla" in lowered["user-agent"]


def test_get_rejects_hosts_outside_allowlist(monkeypatch):
    def boom(*_a, **_k):
        pytest.fail("allowlist 밖 host 로 요청이 나갔다(SSRF)")

    monkeypatch.setattr(us_digest._NOREDIRECT_OPENER, "open", boom)
    assert us_digest._get("evil.example.com", "/x") is None
    assert us_digest._get("api.nasdaq.com", "http://evil/x") is None  # 절대 URL 주입도 차단


# ═══════════════════════════════════════════════════════════════════════════
# ⑪ bridge 배선 — dispatch → _start_digest → _run_digest
# ═══════════════════════════════════════════════════════════════════════════
_TODAY = "2026-07-15"
_US_ITEM = {"id": "us-digest", "on": "session", "channel": "미국주식", "label": "미국주식"}
_OS_ITEM = {"id": "os-digest", "on": "session", "channel": "오픈소스", "label": "오픈소스"}


class _Adapter:
    """dispatch 가 쓰는 최소 계약(role_channel·send)만 구현한 더블."""

    def __init__(self, roles):
        self._roles = roles
        self.sent: list[tuple[int, str, object]] = []
        self.cards: list[object] = []
        self.saves: list[tuple[set, dict]] = []

    def role_channel(self, role):
        return self._roles.get(role)

    def send(self, channel_id, text, buttons=None, card=None):
        self.sent.append((channel_id, text, buttons))
        self.cards.append(card)
        return 1


@pytest.fixture
def us_env(monkeypatch):
    """알림 전역 격리 + #미국주식(777)·#오픈소스(555)·#알림(999) 매핑 + 세션 핑=오늘."""
    bridge.notify_fired.clear()
    bridge.notify_snooze.clear()
    bridge._digest_attempts.clear()
    adapter = _Adapter({"알림": 999, "오픈소스": 555, "미국주식": 777})
    monkeypatch.setattr(
        bridge, "save_notify_state", lambda _p, f, s: adapter.saves.append((set(f), dict(s)))
    )
    monkeypatch.setattr(bridge, "read_session_ping", lambda _p: _TODAY)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, *_a, **_k):
            return datetime(2026, 7, 15, 9, 10, tzinfo=bridge._KST)

    monkeypatch.setattr(bridge, "datetime", _FixedDatetime)
    yield adapter
    bridge.notify_fired.clear()
    bridge.notify_snooze.clear()
    bridge._digest_attempts.clear()


def test_us_digest_registered_as_runner_by_name():
    # 값이 함수 객체면 monkeypatch 교체가 안 먹는다(늦은 바인딩 계약).
    assert bridge.DIGEST_RUNNERS[bridge.US_DIGEST_NOTIFY_ID] == "run_us_digest"
    assert all(isinstance(v, str) for v in bridge.DIGEST_RUNNERS.values())
    assert all(
        callable(globals_get)
        for globals_get in (getattr(bridge, name) for name in bridge.DIGEST_RUNNERS.values())
    )


@pytest.mark.skipif(
    not bridge.SCHEDULES_FILE.exists(),
    reason="배포용 schedules/notify.json 없음 — 공개 미러본에는 익명 example 만 공개된다",
)
def test_deployed_schedule_has_us_digest_on_session():
    items = bridge.load_schedules(bridge.SCHEDULES_FILE)
    item = next((it for it in items if it.get("id") == "us-digest"), None)
    assert item is not None, "배포본 notify.json 에 us-digest 항목이 없다"
    assert item.get("on") == "session" and item.get("channel") == "미국주식"


def test_dispatch_routes_us_digest_to_us_channel(us_env, monkeypatch):
    started = []
    monkeypatch.setattr(bridge, "_start_digest", lambda *a: started.append(a))
    bridge.dispatch_notifications(us_env, [_US_ITEM])
    assert [(a[1], a[2]) for a in started] == [(777, "us-digest")]
    assert us_env.sent == []  # 알림 카드 send 없음(러너가 게시)
    assert ("us-digest", _TODAY) in bridge.notify_fired  # 선기록(틱 중복 차단)


def test_dispatch_starts_both_digests_no_os_regression(us_env, monkeypatch):
    started = []
    monkeypatch.setattr(bridge, "_start_digest", lambda *a: started.append(a))
    bridge.dispatch_notifications(us_env, [_OS_ITEM, _US_ITEM])
    assert [(a[1], a[2]) for a in started] == [(555, "os-digest"), (777, "us-digest")]
    assert {("os-digest", _TODAY), ("us-digest", _TODAY)} <= bridge.notify_fired


def test_dispatch_plain_alert_unaffected_by_us_digest(us_env, monkeypatch):
    # 무회귀: 일반 시각 알림은 종전대로 #알림(999)으로.
    monkeypatch.setattr(
        bridge, "_start_digest", lambda *_a: pytest.fail("일반 알림은 다이제스트 아님")
    )
    item = {"id": "ti-x", "days": ["wed"], "at": "09:00", "grace_min": 30, "label": "L"}
    bridge.dispatch_notifications(us_env, [item])
    assert [c for c, _t, _b in us_env.sent] == [999]


def test_dispatch_us_digest_missing_channel_reverts_then_self_heals(us_env, monkeypatch):
    started = []
    monkeypatch.setattr(bridge, "_start_digest", lambda *a: started.append(a))
    us_env._roles.pop("미국주식")
    bridge.dispatch_notifications(us_env, [_US_ITEM])
    assert ("us-digest", _TODAY) not in bridge.notify_fired  # 다음 틱이 다시 잡는다
    us_env._roles["미국주식"] = 777
    bridge.dispatch_notifications(us_env, [_US_ITEM])
    assert [a[1] for a in started] == [777]


def test_dispatch_us_digest_missing_channel_stops_after_max_attempts(us_env, monkeypatch):
    monkeypatch.setattr(bridge, "_start_digest", lambda *_a: pytest.fail("채널 없이 기동 금지"))
    us_env._roles.pop("미국주식")
    for _ in range(bridge.DIGEST_MAX_ATTEMPTS + 3):
        bridge.dispatch_notifications(us_env, [_US_ITEM])
    assert ("us-digest", _TODAY) in bridge.notify_fired  # 상한 후엔 조용히 포기
    assert bridge._digest_attempts[("us-digest", _TODAY)] == bridge.DIGEST_MAX_ATTEMPTS


def test_digest_attempt_budgets_are_per_id(us_env, monkeypatch):
    # 세션 항목 둘은 **같은 틱에 함께** 돈다 — 예산을 공유하면 os-digest 장애(claude CLI 부재 등)가
    # us-digest 를 한 번도 시도 못 하게 만들고 그날치를 통째로 삼킨다.
    monkeypatch.setattr(bridge, "run_opensource_digest", lambda *_a: False)
    monkeypatch.setattr(bridge, "run_us_digest", lambda *_a: False)
    for _ in range(bridge.DIGEST_MAX_ATTEMPTS):
        bridge._run_digest(us_env, 555, "os-digest", _TODAY)
    assert bridge._digest_attempts[("os-digest", _TODAY)] == bridge.DIGEST_MAX_ATTEMPTS
    bridge.notify_fired.add(("us-digest", _TODAY))
    bridge._run_digest(us_env, 777, "us-digest", _TODAY)  # 남의 소진과 무관하게 첫 시도
    assert ("us-digest", _TODAY) not in bridge.notify_fired  # 되돌아가 다음 틱에 재시도된다
    assert bridge._digest_attempts[("us-digest", _TODAY)] == 1


def test_dispatch_no_session_ping_no_us_digest(us_env, monkeypatch):
    monkeypatch.setattr(bridge, "read_session_ping", lambda _p: None)
    monkeypatch.setattr(bridge, "_start_digest", lambda *_a: pytest.fail("핑 없이 기동 금지"))
    bridge.dispatch_notifications(us_env, [_US_ITEM])
    assert bridge.notify_fired == set()


def test_run_digest_reverts_fired_when_us_runner_fails(us_env, monkeypatch):
    monkeypatch.setattr(bridge, "run_us_digest", lambda *_a: False)
    bridge.notify_fired.add(("us-digest", _TODAY))
    bridge._run_digest(us_env, 777, "us-digest", _TODAY)
    assert ("us-digest", _TODAY) not in bridge.notify_fired
    assert len(us_env.saves) == 1  # 되돌림도 영속


def test_run_digest_reverts_on_us_runner_exception(us_env, monkeypatch):
    def boom(*_a):
        raise RuntimeError("Yahoo 다운")

    monkeypatch.setattr(bridge, "run_us_digest", boom)
    bridge.notify_fired.add(("us-digest", _TODAY))
    bridge._run_digest(us_env, 777, "us-digest", _TODAY)
    assert ("us-digest", _TODAY) not in bridge.notify_fired


def test_run_digest_keeps_fired_on_us_success(us_env, monkeypatch):
    monkeypatch.setattr(bridge, "run_us_digest", lambda *_a: True)
    bridge.notify_fired.add(("us-digest", _TODAY))
    bridge._run_digest(us_env, 777, "us-digest", _TODAY)
    assert ("us-digest", _TODAY) in bridge.notify_fired
    assert us_env.saves == []


def test_run_digest_us_stops_reverting_after_max_attempts(us_env, monkeypatch):
    monkeypatch.setattr(bridge, "run_us_digest", lambda *_a: False)
    for _ in range(bridge.DIGEST_MAX_ATTEMPTS):
        bridge.notify_fired.add(("us-digest", _TODAY))
        bridge._run_digest(us_env, 777, "us-digest", _TODAY)
    assert ("us-digest", _TODAY) in bridge.notify_fired


def test_run_digest_dispatches_to_correct_runner(us_env, monkeypatch):
    # 두 다이제스트가 서로의 러너를 부르면 채널에 엉뚱한 카드가 나간다.
    calls = []
    monkeypatch.setattr(bridge, "run_us_digest", lambda *a: calls.append(("us", a)) or True)
    monkeypatch.setattr(bridge, "run_opensource_digest", lambda *a: calls.append(("os", a)) or True)
    bridge._run_digest(us_env, 777, "us-digest", _TODAY)
    bridge._run_digest(us_env, 555, "os-digest", _TODAY)
    assert [c[0] for c in calls] == ["us", "os"]
    assert calls[0][1][1] == 777 and calls[1][1][1] == 555


# ── run_us_digest 자체 ─────────────────────────────────────────────────────
def test_run_us_digest_posts_card_and_returns_true(us_env, monkeypatch):
    spec = {"title": "T", "fields": [("a", "b", False)], "footer": "f", "color": 1}
    monkeypatch.setattr(bridge.us_digest, "build_us_digest", lambda _d: spec)
    assert bridge.run_us_digest(us_env, 777, _TODAY) is True
    assert us_env.cards == [spec]
    assert us_env.sent[0][0] == 777
    assert "T" in us_env.sent[0][1]  # 카드를 못 그리는 어댑터용 텍스트 폴백도 채워진다


def test_run_us_digest_returns_false_when_card_is_none(us_env, monkeypatch):
    monkeypatch.setattr(bridge.us_digest, "build_us_digest", lambda _d: None)
    assert bridge.run_us_digest(us_env, 777, _TODAY) is False
    assert us_env.sent == []  # 빈 카드를 채널에 흘리지 않는다


def test_run_us_digest_returns_false_when_send_reports_failure(us_env, monkeypatch):
    # 어댑터 계약(§3.3)은 **예외를 던지지 않고 None 을 반환**하는 것이다(플랫폼 오류는 어댑터가
    # 삼키고 로그만). 반환값을 안 보면 게시 실패가 성공으로 나가 fired 가 유지되고, 그날 카드는
    # 0장인데 재시도도 에러도 없다 — 봇 기동 직후 이벤트루프 미준비 틱에서 실제로 나는 경로다.
    monkeypatch.setattr(
        bridge.us_digest, "build_us_digest", lambda _d: {"title": "T", "fields": [], "footer": ""}
    )
    monkeypatch.setattr(us_env, "send", lambda *_a, **_k: None)
    assert bridge.run_us_digest(us_env, 777, _TODAY) is False


def test_run_us_digest_never_calls_claude(us_env, monkeypatch):
    # 미국주식 다이제스트는 판정이 아니라 재료 제공 — LLM 이 낄 자리가 없다(계획서 §0).
    monkeypatch.setattr(bridge, "run_claude", lambda *_a, **_k: pytest.fail("claude 호출 금지"))
    monkeypatch.setattr(
        bridge.us_digest, "build_us_digest", lambda _d: {"title": "T", "fields": [], "footer": ""}
    )
    assert bridge.run_us_digest(us_env, 777, _TODAY) is True
