"""코드 변경 자동 재시작(2026-10-09) — 감시·한가함 판정·마커·🟢 생략.

시계는 주입, 파일은 tmp_path. 실제 종료·디스코드 접속은 없다.
"""

from __future__ import annotations

import asyncio

import bridge
import pytest
from test_bridge import FakeAdapter

discord = pytest.importorskip("discord")
from discord_adapter import DiscordAdapter  # noqa: E402


class _Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def _bump(p, ns=10**9):
    """mtime 을 확실히 달라지게(파일시스템 해상도에 기대지 않는다)."""
    st = p.stat().st_mtime_ns
    import os

    os.utime(p, ns=(st + ns, st + ns))


@pytest.fixture
def code(tmp_path):
    (tmp_path / "a.py").write_text("x=1")
    (tmp_path / "b.py").write_text("y=1")
    sub = tmp_path / "tests"
    sub.mkdir()
    (sub / "t.py").write_text("z=1")
    return tmp_path


def test_snapshot_ignores_subfolders(code):
    assert set(bridge.code_snapshot(code)) == {"a.py", "b.py"}


def test_watcher_waits_for_settle_and_resets_on_new_change(code):
    clk = _Clock()
    w = bridge.ReloadWatcher(code, settle=5, clock=clk)
    assert w.check() is False  # 변경 없음
    _bump(code / "a.py")
    assert w.check() is False  # 감지 직후 — 아직 안정화 전
    clk.t += 4
    assert w.check() is False
    _bump(code / "b.py", 2 * 10**9)  # 4초째에 또 저장 → 타이머 재시작
    assert w.check() is False
    clk.t += 4
    assert w.check() is False
    clk.t += 1
    assert w.check() is True


def test_watcher_ignores_tests_dir_and_revert(code):
    clk = _Clock()
    w = bridge.ReloadWatcher(code, settle=0, clock=clk)
    _bump(code / "tests" / "t.py")
    assert w.check() is False
    a = code / "a.py"
    orig = a.stat().st_mtime_ns
    _bump(a)
    assert w.check() is True
    import os

    os.utime(a, ns=(orig, orig))  # 원복
    assert w.check() is False


class _Music(FakeAdapter):
    def __init__(self, playing=False):
        super().__init__()
        self.playing = playing
        self.closed = 0

    def is_music_active(self):
        return self.playing

    def close(self):
        self.closed += 1


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    monkeypatch.setattr(bridge, "_busy", 0)
    monkeypatch.setattr(bridge, "_reload_requested", False)


def test_idle_false_when_working_or_music_or_digest_thread():
    assert bridge.is_idle(_Music()) is True
    with bridge._working():  # claude 실행/다이제스트 스레드가 안에서 도는 상태
        assert bridge.is_idle(_Music()) is False
    assert bridge.is_idle(_Music()) is True
    assert bridge.is_idle(_Music(playing=True)) is False
    assert bridge.is_idle(FakeAdapter()) is True  # 훅 없는 어댑터는 음악 없음으로


def test_working_released_on_exception():
    with pytest.raises(RuntimeError), bridge._working():
        raise RuntimeError
    assert bridge._busy == 0


def test_run_claude_wrapper_and_digest_count_as_busy(monkeypatch):
    seen = []

    def fake_runner(_adapter, _channel_id, _today):
        seen.append(bridge._busy)
        return True

    monkeypatch.setitem(bridge.DIGEST_RUNNERS, "t-digest", "fake_runner")
    monkeypatch.setattr(bridge, "fake_runner", fake_runner, raising=False)
    bridge._run_digest(FakeAdapter(), 1, "t-digest", "2026-10-09")
    assert seen == [1] and bridge._busy == 0

    @bridge._watch_login
    def fake_claude():
        seen.append(bridge._busy)
        return {"is_error": False, "result": "ok"}

    fake_claude()
    assert seen == [1, 1] and bridge._busy == 0


def test_reload_if_ready_waits_then_writes_marker_and_closes(code, tmp_path):
    clk = _Clock()
    w = bridge.ReloadWatcher(code, settle=0, clock=clk)
    marker = tmp_path / "logs" / "reload_marker"
    ad = _Music()
    assert bridge.reload_if_ready(ad, w, marker) is False  # 변경 없음
    _bump(code / "a.py")
    with bridge._working():
        assert bridge.reload_if_ready(ad, w, marker) is False  # 바쁨 → 대기
    assert not marker.exists() and ad.closed == 0
    ad.playing = True
    assert bridge.reload_if_ready(ad, w, marker) is False  # 음악 중 → 대기
    ad.playing = False
    assert bridge.reload_if_ready(ad, w, marker) is True
    assert marker.exists() and ad.closed == 1 and bridge._reload_requested is True
    assert ad.sent == []  # Off 알림 없음(조용한 재시작)


def test_consume_marker(tmp_path):
    m = tmp_path / "reload_marker"
    assert bridge.consume_reload_marker(m) is False
    m.write_text("reload")
    assert bridge.consume_reload_marker(m) is True
    assert not m.exists()
    assert bridge.consume_reload_marker(m) is False


def test_exit_code_is_not_zero_and_distinct():
    assert bridge.RELOAD_EXIT_CODE == 75


def _adapter(quiet):
    sent = []
    a = DiscordAdapter("tok", [], frozenset({1}), quiet_boot=quiet, clock=_Clock())
    a._channel_map = {88: ("role", "봇상태")}

    async def fake_send(_cid, payload, _view):
        sent.append(payload)
        return 1

    a._send_coro = fake_send  # type: ignore[method-assign]
    return a, sent


def test_boot_notice_skipped_after_auto_reload_but_normal_otherwise():
    a, sent = _adapter(quiet=True)
    asyncio.run(a._announce_ready())
    assert sent == []
    a, sent = _adapter(quiet=False)
    asyncio.run(a._announce_ready())
    assert len(sent) == 1 and "🟢" in sent[0]


def test_reconnect_pair_still_works_when_quiet_boot():
    clk = _Clock()
    a = DiscordAdapter("tok", [], frozenset({1}), quiet_boot=True, clock=clk)
    a._channel_map = {88: ("role", "봇상태")}
    sent = []

    async def fake_send(_cid, payload, _view):
        sent.append(payload)
        return 1

    a._send_coro = fake_send  # type: ignore[method-assign]
    a._ready.set()
    asyncio.run(a._announce_ready())
    asyncio.run(a._client.on_disconnect())
    clk.t += 700
    asyncio.run(a._announce_ready())
    assert len(sent) == 2 and "🔴" in sent[0] and "🟢" in sent[1]


def test_is_music_active_hook():
    a = DiscordAdapter("tok", [], frozenset({1}))
    assert a.is_music_active() is False

    class V:
        def is_connected(self):
            return True

    a._voice = V()
    assert a.is_music_active() is True
