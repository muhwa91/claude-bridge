"""`conftest.IN_MONOREPO` 가 **실제 상황과 일치하는지** 본다.

이 플래그가 잘못 False 로 굳으면 `requires_monorepo` 가 경로 드리프트 방어 3건
(`test_repo_paths_actually_exist`·`test_backlog_read_returns_content`·
`test_yt_dev_log_path_agrees_with_picker`)을 **조용히 skip** 시킨다 — 2026-08-14 실사고
(`BACKLOG_FILE` 만 옛 경로에 남았는데 1,362건이 전부 초록)와 똑같은 무음 실패로 돌아간다.
그래서 skip 스위치 자체에 검사를 하나 건다.

모노레포·공개 미러 **양쪽에서 의미가 있는** 단언이다: 플래그와 실물이 어긋나는 순간 빨개진다.
"""

from conftest import IN_MONOREPO, LIVE_PATHS


def test_flag_agrees_with_reality():
    """플래그 == 「LIVE_PATHS 실물이 전부 있다」."""
    missing = sorted(name for name, p in LIVE_PATHS.items() if not p.exists())
    if IN_MONOREPO:
        assert not missing, f"모노레포로 판정했는데 실물이 없다: {missing} — 폴더가 옮겨졌나?"
    else:
        assert missing, "모노레포 밖으로 판정했는데 실물이 다 있다 — 방어 3건이 헛되이 skip 된다"
