"""#알림·#봇상태 개편(2026-10-08) 케이스 고정 — 이미 단언된 것은 중복 작성하지 않는다.

여기 있는 것은 기존 스위트의 빈칸만이다:
- 경계값(정확히 600초)을 on_disconnect/on_resumed 실흐름으로
- 기동 알림의 실제 전송 경로(_post_status → _send_coro)까지 관통
- 시스템 소식 «전부» 미매핑일 때 예외 없음(한 곳에서 일괄)
- 예약 알림을 dispatch 실흐름으로(프로젝트 채널·텍스트 한 줄·버튼 없음·하루 1회·폴백)
- 옛 `nb:*` 콜백을 어댑터 → 코어 끝까지
- 실제 schedules/notify.json 의 항목 집합

외부 의존(시각·네트워크·claude·디스코드)은 전부 가짜다. 라이브 상태 파일은 conftest 가 격리한다.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime

import bridge
import pytest
from test_bridge import _WED_0910, FakeAdapter, _freeze_now, _item

discord = pytest.importorskip("discord")
import discord_adapter  # noqa: E402
from adapter import Event  # noqa: E402
from discord_adapter import DiscordAdapter  # noqa: E402

_ALLOWED = frozenset({777})
_STATUS_CH = 88
_STAMP_HEAD = r"^\[\d{4}-\d{2}-\d{2} (AM|PM) \d{2}:\d{2}\] "  # notice_stamp 머리(시각 비의존)


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _real_adapter(*, mapped: bool = True):
    """_post_status 는 진짜, 전송(_send_coro)만 가짜인 어댑터. 접속하지 않는다."""
    clock = _Clock()
    a = DiscordAdapter("tok", [], _ALLOWED, clock=clock)
    if mapped:
        a._channel_map = {_STATUS_CH: ("role", "봇상태")}
    sent: list[tuple[int, str, object]] = []

    async def fake_send(cid, payload, view):
        sent.append((cid, payload, view))
        return 1

    a._send_coro = fake_send  # type: ignore[method-assign]
    return a, sent, clock


# ---------------------------------------------------------------------------
# 1·2. 🟢 기동 — 실제 전송 경로 관통, 재-ready 에 반복 없음
# ---------------------------------------------------------------------------
def test_boot_notice_reaches_status_channel_once_with_time(monkeypatch):
    class _FakeDT(datetime):
        @classmethod
        def now(cls, *_a, **_k):
            return datetime(2026, 10, 9, 7, 5)

    monkeypatch.setattr(discord_adapter, "datetime", _FakeDT)
    a, sent, _clock = _real_adapter()
    asyncio.run(a._announce_ready())
    assert sent == [(_STATUS_CH, "[2026-10-09 AM 07:05] 🟢 bridge On", None)]


def test_boot_notice_not_resent_when_on_ready_fires_again():
    a, sent, _clock = _real_adapter()
    a._ready.set()
    asyncio.run(a._announce_ready())
    asyncio.run(a._client.on_disconnect())  # 짧게 끊겼다가(0초)
    asyncio.run(a._announce_ready())  # 재-ready
    asyncio.run(a._announce_ready())
    assert len(sent) == 1 and re.match(_STAMP_HEAD + "🟢 ", sent[0][1])  # 짧은 끊김: Off/On 없음


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(2026, 10, 9, 0, 5), "[2026-10-09 AM 12:05]"),
        (datetime(2026, 10, 9, 11, 59), "[2026-10-09 AM 11:59]"),
        (datetime(2026, 10, 9, 12, 0), "[2026-10-09 PM 12:00]"),
        (datetime(2026, 10, 9, 13, 5), "[2026-10-09 PM 01:05]"),
        (datetime(2026, 1, 2, 23, 59), "[2026-01-02 PM 11:59]"),
    ],
)
def test_notice_stamp_12h_clock_boundaries(now, expected):
    assert bridge.notice_stamp(now) == expected


def test_notice_stamp_defaults_to_kst_now():
    assert re.match(_STAMP_HEAD, bridge.notice_stamp() + " ")  # 인자 없이도 형식 유지


def test_boot_notice_is_stamp_then_green_text():
    now = datetime(2026, 10, 9, 13, 5)
    assert discord_adapter.boot_notice_text(now) == "[2026-10-09 PM 01:05] 🟢 bridge On"


def test_giveup_notice_is_two_lines():
    for item_id in (bridge.US_DIGEST_NOTIFY_ID, bridge.SPOTIFY_NOTIFY_ID, "x-unknown"):
        first, arrow = bridge.digest_giveup_text(item_id).split("\n")
        assert re.match(_STAMP_HEAD + "⛔ ", first) and first.endswith(" 생성 실패")
        assert arrow == "→ claude-bridge/logs/bridge.log"


def test_notify_text_is_two_lines_only_with_note():
    first, arrow = bridge.notify_text({"label": "L", "note": "N"}).split("\n")
    assert re.match(_STAMP_HEAD + "⏰ <스케쥴> L$", first) and arrow == "→ N"
    assert "\n" not in bridge.notify_text({"label": "L"})


class _StopMain(Exception):
    """main() 을 set_login_alert 배선 지점에서 끊는 신호."""


def _login_hook_from_main(monkeypatch):
    """실제 main() 배선이 set_login_alert 에 넘기는 콜러블을 가로챈다(접속 전에 중단)."""
    from types import SimpleNamespace

    captured: dict[str, object] = {}

    def fake_set(hook):
        captured["hook"] = hook
        raise _StopMain

    monkeypatch.setattr(bridge, "setup_logging", lambda: None)
    monkeypatch.setattr(
        bridge,
        "load_env",
        lambda _p: {"DISCORD_BOT_TOKEN": "tok", "DISCORD_ALLOWED_USER_IDS": "777"},
    )
    monkeypatch.setattr(bridge.shutil, "which", lambda _n: "claude-not-run")
    monkeypatch.setattr(bridge, "acquire_lock", lambda _p: True)
    monkeypatch.setattr(bridge, "load_schedules", lambda _p: [])
    monkeypatch.setattr(bridge, "load_notify_state", lambda *_a: set())
    monkeypatch.setattr(bridge, "load_channel_sessions", lambda _p: {})
    monkeypatch.setattr(bridge, "list_projects", lambda _r: [])
    monkeypatch.setattr(bridge, "set_login_alert", fake_set)
    monkeypatch.setattr(bridge, "youtube", SimpleNamespace(), raising=False)
    with pytest.raises(_StopMain):
        bridge.main()
    return captured["hook"]


def test_every_system_notice_starts_with_the_stamp(monkeypatch):
    """기동·재연결·로그인·포기·예약 — 전부 `[YYYY-MM-DD AM|PM hh:mm] ` 머리로 시작한다."""
    # 기동·재연결: 실제 어댑터 전송 경로(둘 다 _post_status)
    a, sent, clock = _real_adapter()
    a._ready.set()
    asyncio.run(a._announce_ready())
    asyncio.run(a._client.on_disconnect())
    clock.t += 700
    asyncio.run(a._client.on_resumed())
    boot, off, back = (p for _c, p, _v in sent)
    assert "🟢" in boot and "🔴 bridge Off" in off and "🟢 bridge On" in back
    assert off == f"{bridge.notice_stamp(datetime.fromtimestamp(1000.0))} 🔴 bridge Off"
    # 재시작 경로의 Off(bridge.off_notice_text)도 같은 머리 규칙
    restart_off = bridge.off_notice_text()

    # 로그인: main() 이 실제로 배선한 콜러블을 호출(전송은 FakeAdapter 로 받는다)
    posted: list[str] = []
    monkeypatch.setattr(bridge, "post_system_notice", lambda _a, text: posted.append(text) or True)
    hook = _login_hook_from_main(monkeypatch)
    assert callable(hook) and hook() is True
    (login,) = posted
    assert login.endswith(" 🔐 Claude 로그인 필요")

    # 포기·예약
    giveup = bridge.digest_giveup_text(bridge.US_DIGEST_NOTIFY_ID)
    sched = bridge.notify_text({"label": "L", "note": "N"})
    assert "⛔" in giveup and "⏰" in sched

    for text in (boot, off, back, restart_off, login, giveup, sched):
        assert re.match(_STAMP_HEAD, text), text


# ---------------------------------------------------------------------------
# 3·4. 🔌 경계 — «600초 이상»이 보낸다(미만은 조용)
# ---------------------------------------------------------------------------
def test_reconnect_exactly_600s_is_reported_inclusive_boundary():
    a, sent, clock = _real_adapter()
    a._ready.set()
    asyncio.run(a._client.on_disconnect())
    clock.t += 600  # 정확히 10분
    asyncio.run(a._client.on_resumed())
    assert [(c, p) for c, p, _v in sent] == [
        (_STATUS_CH, f"{bridge.notice_stamp(datetime.fromtimestamp(1000.0))} 🔴 bridge Off"),
        (_STATUS_CH, f"{bridge.notice_stamp(datetime.fromtimestamp(1600.0))} 🟢 bridge On"),
    ]  # Off 먼저, On 나중 — 둘 다 #봇상태


def test_reconnect_599s_is_silent_and_nine_minutes_is_silent():
    for gap in (599, 9 * 60):
        a, sent, clock = _real_adapter()
        a._ready.set()
        asyncio.run(a._client.on_disconnect())
        clock.t += gap
        asyncio.run(a._client.on_resumed())
        assert sent == [], gap


# ---------------------------------------------------------------------------
# 7. 로그인 패턴 — 정상 결과 본문의 낱말은 무시 / 일반 실패도 무시
# ---------------------------------------------------------------------------
@pytest.fixture
def login_hook(monkeypatch):
    calls: list[int] = []
    bridge._login_alert_day = ""
    bridge.set_login_alert(lambda: calls.append(1) or True)
    _freeze_now(monkeypatch, _WED_0910)
    yield calls
    bridge.set_login_alert(None)
    bridge._login_alert_day = ""


@pytest.mark.parametrize(
    "data",
    [
        {"is_error": False, "result": "OAuth authentication 설명: /login 도 있습니다"},
        {"result": "authentication"},  # is_error 키 자체가 없음
        {"is_error": None, "result": "Not logged in"},
        {"is_error": True, "result": "타임아웃(900s) 초과"},
        {"is_error": True, "result": ""},
    ],
)
def test_login_alert_silent_for_non_auth_cases(login_hook, data):
    bridge._report_login_expired(data)
    assert login_hook == []


def test_login_alert_through_watch_login_with_realistic_error_text(login_hook):
    @bridge._watch_login
    def fake_run_claude():
        return {"is_error": True, "result": "Invalid API key · Please run /login"}

    fake_run_claude()
    fake_run_claude()  # 같은 날 두 번째
    assert login_hook == [1]


# ---------------------------------------------------------------------------
# 9·10. ⏰ 예약 알림 — dispatch 실흐름
# ---------------------------------------------------------------------------
@pytest.fixture
def sched_env(monkeypatch):
    bridge.notify_fired.clear()
    fa = FakeAdapter(secrets=[], roles={"봇상태": 999}, projects={"trading-info": 111})
    monkeypatch.setattr(bridge, "save_notify_state", lambda _p, f: fa.saves.append(set(f)))
    yield fa
    bridge.notify_fired.clear()


def test_scheduled_alert_goes_to_project_channel_as_one_plain_line_once_per_day(
    sched_env, monkeypatch
):
    _freeze_now(monkeypatch, _WED_0910)  # at 09:00 창(30분) 안
    items = [_item(id="a", project="trading-info", label="제목", note="내용")]
    bridge.dispatch_notifications(sched_env, items)
    bridge.dispatch_notifications(sched_env, items)  # 같은 날 두 번째 틱
    _freeze_now(monkeypatch, _WED_0910.replace(minute=20))  # 같은 날, 창 안의 더 늦은 틱
    bridge.dispatch_notifications(sched_env, items)
    assert sched_env.sent == [
        (111, "[2026-07-15 AM 09:10] ⏰ <스케쥴> 제목\n→ 내용", None)
    ]  # 채널 1곳·텍스트 한 줄·버튼 없음(두 번째·세 번째 틱은 fired 로 막힘)
    assert sched_env.sent[0][1].count("\n→ ") == 1  # 두 줄(제목 / → 내용)


def test_scheduled_alert_unmapped_project_falls_back_to_status_channel(sched_env, monkeypatch):
    _freeze_now(monkeypatch, _WED_0910)
    bridge.dispatch_notifications(
        sched_env, [_item(id="a", project="없는프로젝트", label="제목", note="내용")]
    )
    assert sched_env.sent == [(999, "[2026-07-15 AM 09:10] ⏰ <스케쥴> 제목\n→ 내용", None)]


# ---------------------------------------------------------------------------
# 11. #봇상태 미매핑 — 시스템 소식 전부(🟢·🔌·🔐·⛔) + ⏰ 폴백: 예외 없음, 로그만
# ---------------------------------------------------------------------------
def test_unmapped_status_channel_boot_is_log_only(caplog):
    a, sent, _clock = _real_adapter(mapped=False)
    with caplog.at_level(logging.WARNING, logger="bridge"):
        asyncio.run(a._announce_ready())
    assert sent == [] and "미매핑" in caplog.text


def test_unmapped_status_channel_reconnect_is_log_only(caplog):
    a, sent, clock = _real_adapter(mapped=False)
    a._ready.set()
    asyncio.run(a._client.on_disconnect())
    clock.t += 1200
    with caplog.at_level(logging.WARNING, logger="bridge"):
        asyncio.run(a._client.on_resumed())
    assert sent == [] and "미매핑" in caplog.text and a._disconnected_at is None


@pytest.mark.usefixtures("login_hook")
def test_unmapped_status_channel_login_expired_is_log_only(caplog):
    fa = FakeAdapter()  # 봇상태 없음
    bridge.set_login_alert(lambda: bridge.post_system_notice(fa, bridge.LOGIN_EXPIRED_TEXT))
    with caplog.at_level(logging.WARNING, logger="bridge"):
        bridge._report_login_expired({"is_error": True, "result": "Please run /login"})
    assert fa.sent == [] and "미매핑" in caplog.text
    # 전송 실패(False)로 돌려줬으니 같은 날 재시도 가능 상태로 풀려 있어야 한다
    assert bridge._login_alert_day == ""


def test_unmapped_status_channel_digest_giveup_is_log_only(sched_env, monkeypatch, caplog):
    sched_env._roles = {"미국주식": 555}  # 봇상태만 없음
    bridge._digest_attempts.clear()
    monkeypatch.setattr(bridge, "run_us_digest", lambda *_a: False)
    with caplog.at_level(logging.WARNING, logger="bridge"):
        for _ in range(bridge.DIGEST_MAX_ATTEMPTS):
            bridge.notify_fired.add(("us-digest", "2026-07-15"))
            bridge._run_digest(sched_env, 555, "us-digest", "2026-07-15")
    assert sched_env.sent == [] and "미매핑" in caplog.text
    bridge._digest_attempts.clear()


def test_unmapped_status_channel_scheduled_alert_skipped_without_raise(sched_env, monkeypatch):
    _freeze_now(monkeypatch, _WED_0910)
    sched_env._roles = {}
    sched_env._projects = {}
    bridge.dispatch_notifications(sched_env, [_item(id="a", project="x")])  # 폴백처도 없음
    assert sched_env.sent == []


# ---------------------------------------------------------------------------
# 12. 옛 검증 카드 콜백 — 어댑터 → 코어 끝까지 무송신
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "custom_id", ["nb:ok:x", "nb:later:x", "nb:done:x", "nb:handoff:x", "nb:confirm:x", "nb:"]
)
def test_old_verification_card_click_is_acked_and_ignored_end_to_end(custom_id, tmp_path):
    from types import SimpleNamespace

    async def defer():
        pass

    inter = SimpleNamespace(
        type=discord.InteractionType.component,
        user=SimpleNamespace(id=777),
        response=SimpleNamespace(defer=defer),
        data={"custom_id": custom_id},
        id=9001,
        message=SimpleNamespace(id=42),
        channel_id=100,
    )
    a = DiscordAdapter("tok", [], _ALLOWED)
    asyncio.run(a._on_interaction(inter))
    ev = a._queue.get_nowait()
    assert isinstance(ev, Event) and ev.kind == "button" and ev.action == ""  # 화이트리스트 밖

    fa = FakeAdapter()
    bridge.handle_event(
        fa,
        ev,
        allowed=_ALLOWED,
        claude_exe="claude-not-run",
        repo_root=tmp_path,
        target_root=str(tmp_path),
        timeout=1,
    )
    assert fa.sent == [] and fa.edited == [] and fa.runs == []
    assert len(fa.acked) == 1  # 로딩 스피너만 끈다


# ---------------------------------------------------------------------------
# 13. 옛 notify.json 의 pending-checks — 정확히 그 모양(id+on 만)
# ---------------------------------------------------------------------------
def test_legacy_pending_checks_entry_sends_nothing_and_records_no_fired(
    sched_env, monkeypatch, tmp_path
):
    # on:"session" 은 로더가 버린다 — 파일에 남은 옛 항목이 알림으로 새거나 fired 를 찍지 않는다.
    _freeze_now(monkeypatch, _WED_0910)
    f = tmp_path / "notify.json"
    f.write_text('{"items": [{"id": "pending-checks", "on": "session"}]}', encoding="utf-8")
    monkeypatch.setattr(bridge, "SCHEDULES_FILE", f)
    bridge.dispatch_notifications(sched_env)
    assert sched_env.sent == [] and bridge.notify_fired == set() and sched_env.saves == []


# ---------------------------------------------------------------------------
# 15. 실제 schedules/notify.json — 정확히 3건
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not bridge.SCHEDULES_FILE.exists(), reason="배포용 notify.json 없음(공개 미러)")
def test_real_notify_json_has_exactly_three_items():
    items = bridge.load_schedules(bridge.SCHEDULES_FILE)
    assert sorted(it["id"] for it in items) == [
        "agent-usage-compare",
        "spotify-monthly",
        "us-digest",
    ]
    assert "pending-checks" not in {it["id"] for it in items}
