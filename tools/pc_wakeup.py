#!/usr/bin/env python3
"""PC활성화 필요 알림 — 그날 시각 알림이 있으면 폰(텔레그램)으로 미리 알린다.

왜 브리지가 아니라 여기인가:
  디스코드 카드는 **브리지가 떠 있어야** = PC 가 켜져 있어야 뜬다. 그런데 이 알림이
  필요한 상황이 정확히 "PC 가 꺼져 있는 아침"이라 순환이다. 그래서 GitHub Actions 가
  레포의 schedules/notify.json 을 읽어 텔레그램으로 미리 보낸다(2026-08-12 운영자 지시).

정시성은 요구하지 않는다:
  cron 이 늦어도 **확인가능 창(대개 08:30~) 전에만** 닿으면 목적을 다한다 — 목적은
  "일어나서 씻을 시간을 버는 것"이다. 그래서 GAS dispatch 없이 cron 하나로 간다
  (etf-info 는 08:30 정시 발송이 목표라 GAS 가 필요했지만 여기는 아니다).

bridge.py 는 이 파일을 부르지 않는다 — tools/yt_pick.py 와 같은 자리다(브리지 코드가
아니라, 실행 호스트·파이썬 게이트를 공유해서 여기 있을 뿐). 어댑터·코어에 배선하지 말 것.

점검: python tools/pc_wakeup.py --selftest   (⚠️ 인자 없이 실행하면 실제로 전송한다)
"""

import json
import os
import re
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
NOTIFY = Path(__file__).resolve().parent.parent / "schedules" / "notify.json"

# notify.json 의 days 표기. datetime.weekday() 가 월=0 이라 순서를 그대로 맞춘다.
DOW = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

PROJECT_MARK = "💼"  # 폰으로 오는 알림 3종(발송이상·테넌시·PC활성화) 공통 표식


# `\s` 를 쓰면 개행까지 먹어 '-#\n다음 줄' 이 한 줄로 합쳐진다 → 같은 줄의 공백만([ \t]).
_MD = re.compile(r"\*\*|`|^-#[ \t]*", re.M)  # 디스코드 전용 마크업(굵게·코드·서브텍스트)


def to_plain(text):
    """디스코드 문구 → 텔레그램용 순수 텍스트(parse_mode 없이 보내므로 기호가 그대로 보인다)."""
    return _MD.sub("", re.sub(r"<(https?://[^>\s]+)>", r"\1", text))


def mask(text, secret):
    """예외 메시지에서 시크릿을 가린다 — repr 로 escape 된 형태까지 함께 지운다."""
    if not secret:  # replace("", ...) 는 글자 사이마다 끼워 넣어 문자열을 망가뜨린다
        return text
    return text.replace(secret, "***").replace(repr(secret)[1:-1], "***")


# 쌍둥이: etf-info/check_morning_send.py · 공개 레포 oci_arm_grabber/check_tenancy.py 의
# tg()/to_plain()/mask() — 한쪽만 고치지 마라(레포·프로젝트가 갈려 있어 공유 모듈은 못 만든다)
def tg(msg):
    """텔레그램 전송(수신 전용 봇). 실패는 삼키되 반드시 로그에 남긴다."""
    token = os.environ.get("TELEGRAM_DEV_BOT_TOKEN", "")
    chat = os.environ.get("TELEGRAM_DEV_CHAT_ID", "")
    if not token or not chat:
        print("telegram skipped: 미설정")
        return False
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        # UTF-8 명시 필수 — cp949 로 나가면 'strings must be encoded in UTF-8' 로 거절된다(실측).
        data=json.dumps({"chat_id": chat, "text": to_plain(msg)}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=15)
        return True
    except Exception as e:
        # 토큰이 URL 경로에 들어가므로 예외 메시지에 평문으로 실린다 → 반드시 가리고 찍는다.
        print(f"telegram notify failed: {type(e).__name__}: {mask(str(e), token)}")
        return False


def kdate(d):
    """머리글 날짜 `26년 8월 12일`. strftime 의 `%-m`(리눅스 전용)을 피해 직접 조립한다."""
    return f"{d.year % 100}년 {d.month}월 {d.day}일"


def add_min(hhmm, minutes):
    """'07:50' + 70 → '09:00'. 아침 알림 전용이라 자정 넘김은 상정하지 않는다."""
    h, m = (int(x) for x in hhmm.split(":"))
    t = h * 60 + m + int(minutes)
    return f"{t // 60 % 24:02d}:{t % 60:02d}"


def hm(hhmm):
    """'08:30' → 830. 시각 비교용(문자열 비교는 '9:00' 같은 표기에서 어긋난다)."""
    h, m = hhmm.split(":")
    return int(h) * 100 + int(m)


def due(items, today, now_hm=None):
    """오늘 발화할 **시각 알림**만 고른다.

    `at` 이 없는 항목(`on: "session"` 다이제스트·리마인더)은 PC 앞에 있어야 할 시각이
    없으므로 애초에 대상이 아니다. `days` 가 없으면 매일로 본다 — 브리지
    `due_notifications` 와 같은 규약이라 한쪽만 다르게 해석하면 안 된다.

    now_hm: 실행 시각(KST HHMM). 주면 **이미 끝난 건을 뺀다.** cron 은 크게 늦을 수
    있어서(같은 레포 실측 08:30→11:03) 늦게 깬 날 "지나간 일"을 알릴 수 있다.
    ⚠️ 기준은 `at` 이 아니라 **창의 끝**이다 — at(07:50)이 지났어도 확인가능 창
    (~09:00)이 열려 있으면 알림은 여전히 쓸모 있다. `check_to` 가 없으면 PC활성화
    종료(at+grace)를 창의 끝으로 본다. None 이면 시각을 안 따진다(순수 필터).
    """
    dow = DOW[today.weekday()]
    out = [i for i in items if i.get("at") and dow in (i.get("days") or DOW)]
    if now_hm is None:
        return out
    return [
        i for i in out if hm(i.get("check_to") or add_min(i["at"], i.get("grace_min", 0))) > now_hm
    ]


def render(item, today):
    """항목 하나 → 폰에 뜨는 본문. 프로젝트마다 따로 보낸다(💼 가 공통이라 묶으면 흐려진다)."""
    lines = [
        f"[{kdate(today)}]",
        f"{PROJECT_MARK} {item.get('project') or item['id']}",
        "💻 PC활성화 필요",
        f"- PC활성화 : {item['at']}~{add_min(item['at'], item.get('grace_min', 0))}",
    ]
    if item.get("check_from") and item.get("check_to"):
        lines.append(f"- 확인가능 : {item['check_from']}~{item['check_to']}")
    lines.append("- 디스코드 - 해당 프로젝트 채널 [확인시작]")
    if item.get("prep"):  # 그 판정에 미리 띄워둬야 하는 것(예: 백엔드 서버)
        lines.append(f"- ⚠️ {item['prep']}")
    return "\n".join(lines)


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8")).get("items", [])


def selftest():
    wed = date(2026, 8, 12)  # 수요일
    sat = date(2026, 8, 15)

    assert add_min("07:50", 70) == "09:00"
    assert add_min("07:50", 0) == "07:50"
    assert add_min("23:30", 60) == "00:30", "자정 넘김도 죽지는 않는다(값만 확인)"
    assert kdate(wed) == "26년 8월 12일"

    ti = {
        "id": "ti-premarket-baseline",
        "project": "trading-info",
        "days": ["mon", "tue", "wed", "thu", "fri"],
        "at": "07:50",
        "grace_min": 70,
        "check_from": "08:30",
        "check_to": "09:00",
        "prep": "백엔드 실행 필요 → 바탕화면 [주식] 아이콘",
    }
    session_item = {"id": "us-digest", "on": "session", "days": ["wed"]}
    everyday = {"id": "x", "project": "p", "at": "06:00"}  # days 없음 = 매일

    # ① 시각 없는 항목은 대상이 아니다 — 이걸 놓치면 다이제스트까지 폰으로 나간다.
    assert due([session_item], wed) == []
    # ② 요일 필터. 토요일엔 평일 항목이 빠진다.
    assert [i["id"] for i in due([ti, session_item], wed)] == ["ti-premarket-baseline"]
    assert due([ti], sat) == []
    # ③ days 없으면 매일 — 브리지와 같은 규약.
    assert [i["id"] for i in due([everyday], sat)] == ["x"]
    # ④ 창이 닫힌 건은 뺀다(cron 이 크게 늦게 깬 날). 경계는 check_to = 09:00.
    assert [i["id"] for i in due([ti], wed, 605)] == ["ti-premarket-baseline"], "정시 발화"
    assert [i["id"] for i in due([ti], wed, 800)] == ["ti-premarket-baseline"], (
        "at 은 지났어도 창은 열림"
    )
    assert [i["id"] for i in due([ti], wed, 859)] == ["ti-premarket-baseline"], "창 끝 직전"
    assert due([ti], wed, 900) == [], "창 끝 = 더 알릴 이유 없음"
    assert due([ti], wed, 1103) == [], "실측 최악 지연(11:03)에도 헛알림이 안 나간다"
    # check_to 가 없으면 PC활성화 종료(at+grace)가 창의 끝이다.
    assert due([{"id": "n", "at": "07:00", "grace_min": 30}], wed, 725) != []
    assert due([{"id": "n", "at": "07:00", "grace_min": 30}], wed, 730) == []

    body = render(ti, wed)
    assert body == (
        "[26년 8월 12일]\n"
        "💼 trading-info\n"
        "💻 PC활성화 필요\n"
        "- PC활성화 : 07:50~09:00\n"
        "- 확인가능 : 08:30~09:00\n"
        "- 디스코드 - 해당 프로젝트 채널 [확인시작]\n"
        "- ⚠️ 백엔드 실행 필요 → 바탕화면 [주식] 아이콘"
    ), body
    # ⑤ 선택 필드가 없으면 그 줄만 빠진다(빈 값·None 이 본문에 새지 않는다).
    bare = render({"id": "bare", "at": "06:00"}, wed)
    assert bare.splitlines()[1] == "💼 bare", bare
    assert "확인가능" not in bare and "⚠️" not in bare, bare

    # ⑥ 실제 notify.json 이 읽히고 스키마가 맞는지(항목이 0건이어도 파싱은 성립해야 한다).
    items = load(NOTIFY)
    assert isinstance(items, list) and all("id" in i for i in items), items
    for i in due(items, wed):
        render(i, wed)  # 배포본 항목이 렌더 중 죽지 않는지

    # ⑦ tg() — cp949 사고가 났던 함수라 실제 Request 를 검사한다(네트워크 안 나감).
    seen = {}

    def fake(req, **_):  # urlopen(req, timeout=…) 의 키워드를 흡수한다
        seen["url"] = req.full_url
        seen["body"] = json.loads(req.data.decode("utf-8"))
        import io

        return io.BytesIO(b'{"ok":true}')

    real, urllib.request.urlopen = urllib.request.urlopen, fake
    try:
        os.environ["TELEGRAM_DEV_BOT_TOKEN"] = "T:1"
        os.environ["TELEGRAM_DEV_CHAT_ID"] = "9"
        assert tg("**굵게** 한글") is True
        assert seen["body"]["text"] == "굵게 한글", seen
        assert "T:1" in seen["url"]
        os.environ["TELEGRAM_DEV_BOT_TOKEN"] = ""
        seen.clear()
        assert tg("x") is False and not seen, "미설정이면 전송하지 않는다"
    finally:
        urllib.request.urlopen = real
        os.environ.pop("TELEGRAM_DEV_BOT_TOKEN", None)
        os.environ.pop("TELEGRAM_DEV_CHAT_ID", None)

    print("selftest ok")


def main():
    # 판정 결과에 이모지가 섞인다 — 콘솔이 cp949 인 로컬에서 print 가 죽는 걸 막는다.
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if "--selftest" in sys.argv:
        selftest()
        return
    now = datetime.now(KST)
    today = now.date()
    picked = due(load(NOTIFY), today, now.hour * 100 + now.minute)
    if not picked:
        # 0건이면 아무것도 보내지 않는다 — 매일 "없음" 이 오면 그게 소음이고,
        # 그러면 진짜 올 날 안 보게 된다(발송이상·테넌시 알림과 같은 규약).
        print(f"{today} PC활성화 필요 항목 없음 — 발신하지 않는다.")
        return
    failed = 0
    for item in picked:
        body = render(item, today)
        print(body)
        if not tg(body):
            failed += 1
    if failed:
        # 전송 실패를 삼키면 러너는 초록인데 폰엔 아무것도 안 온다 = 감시가 없는 것과 같다.
        sys.exit(1)


if __name__ == "__main__":
    main()
