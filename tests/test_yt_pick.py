"""`tools/yt_pick.py` 의 yt-dlp 호출 규약 — **콘솔 창을 띄우지 않는다**.

2026-08-28 실사고: 세션 훅(`.claude/hooks/yt-daily.mjs`)이 python 을
`detached: true, stdio: 'ignore', windowsHide: true` 로 던지는데 **windowsHide 는 python
프로세스까지만** 먹는다. 콘솔 없는 부모 밑에서 손자인 `yt-dlp.exe` 가 새 콘솔을 배정받아
빈 창이 떴다 — 사용자가 그 창을 닫으면 자식 트리가 `0xC000013A` 로 죽는다.
그래서 `yt()` 가 `creationflags=CREATE_NO_WINDOW` 를 **직접** 넘긴다.

네트워크·실제 yt-dlp 실행 없음 — `subprocess.run` 을 갈아끼워 **전달된 kwargs 만** 본다.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys

import bridge  # conftest 가 sys.path 에 프로젝트 루트를 넣은 뒤에만 임포트 가능
import pytest

_WIN_NO_WINDOW = 0x08000000  # winbase.h CREATE_NO_WINDOW — 값이 바뀌면 이 상수도 틀린다


@pytest.fixture(scope="module")
def yt_pick():
    """`tools/yt_pick.py` 를 파일 경로로 로드한다(패키지가 아니라 스크립트라 import 가 안 된다).

    `test_bridge.py::test_yt_dev_log_path_agrees_with_picker` 와 같은 방식이다.
    모듈 최상위는 Path 계산·상수뿐이라 로드 자체에 부작용이 없다(선별은 `main()` 안에서만 돈다).
    """
    spec = importlib.util.spec_from_file_location(
        "yt_pick", bridge.PROJECT_DIR / "tools" / "yt_pick.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def run_spy(monkeypatch, yt_pick):
    """`subprocess.run` 을 기록기로 갈아끼운다 → `[(cmd, kwargs), ...]`.

    반환 종료코드는 0 — 0 이 아니면 `yt()` 가 모듈 전역 `ERRORS` 에 남겨 다음 테스트로 샌다.
    """
    calls: list[tuple[list[str], dict]] = []

    def fake_run(cmd, **kw):
        calls.append((cmd, kw))
        return subprocess.CompletedProcess(cmd, 0, "STDOUT", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(yt_pick, "ERRORS", [])
    return calls


def _call(yt_pick):
    return yt_pick.yt(["--dump-json", "https://example/x"], yt_pick.Budget(5), "테스트")


def test_yt_passes_creationflags_to_yt_dlp(yt_pick, run_spy):
    """케이스 1 — `yt()` 가 `creationflags` 를 넘긴다(빠지면 콘솔 창이 뜬다)."""
    assert _call(yt_pick) == "STDOUT"
    cmd, kw = run_spy[0]
    assert cmd[0] == "yt-dlp"
    assert "creationflags" in kw, "creationflags 가 빠졌다 — detached 실행에서 콘솔 창이 뜬다"
    assert kw["creationflags"] == getattr(subprocess, "CREATE_NO_WINDOW", 0)


@pytest.mark.skipif(sys.platform != "win32", reason="CREATE_NO_WINDOW 는 윈도우 전용 상수")
def test_yt_creationflags_is_create_no_window(yt_pick, run_spy):
    """윈도우에선 그 값이 실제로 CREATE_NO_WINDOW(0x08000000) 여야 한다."""
    _call(yt_pick)
    assert run_spy[0][1]["creationflags"] == _WIN_NO_WINDOW


def test_yt_falls_back_to_zero_without_the_constant(yt_pick, run_spy, monkeypatch):
    """케이스 3 — 상수가 없는 환경(비윈도우)에서도 예외 없이 `creationflags=0` 으로 떨어진다.

    `subprocess.CREATE_NO_WINDOW` 를 지워 리눅스·macOS 를 흉내낸다. 직접 참조로 바꾸면
    여기서 AttributeError 가 나면서 리눅스 CI 의 yt-dlp 호출이 통째로 죽는다.
    """
    monkeypatch.delattr(subprocess, "CREATE_NO_WINDOW", raising=False)
    assert _call(yt_pick) == "STDOUT"
    assert run_spy[0][1]["creationflags"] == 0
