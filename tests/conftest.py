"""tests/ 에서 프로젝트 루트의 bridge.py 를 임포트할 수 있게 sys.path 에 루트를 추가."""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import bridge  # sys.path 주입 뒤에만 임포트 가능

# 아래 격리 fixture 가 덮기 **전의** 실경로. 상수가 가리키는 파일이 실제로 있는지 보는
# 테스트(`test_repo_paths_actually_exist`)가 이걸 쓴다 — fixture 가 autouse 라 그냥 읽으면
# tmp 경로가 나와 **검사가 통째로 무의미해진다**(2026-08-14 실사고: `_Core/` 재배치 때
# 경로 상수 하나만 옛 경로에 남았는데 테스트 1,362건이 전부 통과했다. 쓰는 테스트가
# 하나같이 monkeypatch 해서 실경로를 아무도 안 봤기 때문).
LIVE_PATHS = {
    "JUDGE_PROJECT_DIR": bridge.JUDGE_PROJECT_DIR,  # launch_judge 의 cwd — 없으면 판정 입구 실패
    "SNS_INBOX_DIR": bridge.SNS_INBOX_DIR,  # 옵시디언 수집함 — 없으면 노트가 엉뚱한 곳에 쌓인다
}

# 위 경로는 **모노레포 안에서만** 실물이 있다. 공개 미러(`muhwa91/claude-bridge`)는 이 프로젝트
# 폴더만 떼어낸 독립 클론이라 `REPO_ROOT/_System/` 이 아예 없고, 실경로를 보는 테스트들이
# 거기서는 «구조적으로 통과 불가»한 빨간불로 남는다(2026-08-30 실측 — 공개 레포를 보는 사람에게는
# 깨진 프로젝트로 보인다). 그래서 «없다»를 값으로 만들어 두고 그 테스트만 skip 한다.
# 🔴 **모노레포 안에서는 절대 skip 되면 안 된다** — 그 테스트들이 경로 드리프트의 유일한 방어선이라
# (2026-08-14 실사고) 조용히 꺼지면 종전과 똑같은 무음 실패로 돌아간다.
# 회귀: `test_conftest_monorepo_guard.py`.
IN_MONOREPO = (bridge.REPO_ROOT / "_System" / "Core").is_dir()

requires_monorepo = pytest.mark.skipif(
    not IN_MONOREPO, reason="모노레포 밖(공개 미러 클론) — LIVE_PATHS 실물이 없다"
)

ORIG_SNS_SPAWN = bridge._sns_spawn  # 아래 격리 fixture 가 갈아끼우기 전의 진짜(스레드) 구현

# 브리지가 **실제로 쓰는** 상태 파일. 모듈 상수를 직접 읽는 함수를 부르는 테스트가 monkeypatch 를
# 빠뜨리면 라이브가 오염된다.
_STATE_ATTRS = (
    # 스포티파이 월 스탬프(2026-08-25) — **수동 'ㅁ스포티파이' 테스트도 이 파일을 쓴다**
    # (_handle_music_spotify 가 직접 찍는다). 격리를 빠뜨리면 테스트가 라이브 스탬프를 이번 달로
    # 덮어 **그 달의 자동 실행이 통째로 사라진다**.
    "SPOTIFY_MONTH_F",
    "RELOAD_MARKER",  # 자동 재시작 마커 — 라이브 logs/ 에 남기면 다음 기동이 🟢 를 건너뛴다
    # SNS 수집 — 상태·완료 신호·**옵시디언 수집함**(빠뜨리면 테스트 노트가 실제 수집함에 쌓인다).
    "SNS_STATE_FILE",
    "SNS_DONE_FILE",
    "SNS_INBOX_DIR",
)


# 아래 fixture 가 갈아끼우기 **전**의 실제 경로 — 임포트 시점에 잡아둔다.
# (옛 `ORIGINAL_PATHS` 는 위 LIVE_PATHS 와 목적이 같아 2026-08-14 통합했다 — 같은 일을 하는
#  dict 가 둘이면 다음 사람이 틀린 쪽에 상수를 늘린다.)


@pytest.fixture(autouse=True)
def _isolate_state_files(monkeypatch, tmp_path_factory):
    """상태 파일 전부를 **모든 테스트에서** tmp 로 돌린다(라이브 오염 방지 가드).

    개별 테스트가 자기 경로로 다시 덮는 것은 자유(이 fixture 가 먼저 깔린다).
    tmp_path **밖**에 둔다 — tmp_path 를 프로젝트 루트로 쓰는 테스트(list_projects)가 있어
    거기에 폴더를 만들면 가짜 프로젝트로 잡힌다.
    """
    # SNS 느린 작업의 데몬 스레드를 테스트에선 그 자리에서 돌린다(결과를 바로 단언하려고).
    # 실제 스레드로 띄우는지는 ORIG_SNS_SPAWN 으로 따로 검사한다(test_bridge).
    monkeypatch.setattr(bridge, "_sns_spawn", lambda _name, fn, *args: fn(*args))
    state = tmp_path_factory.mktemp("state")
    for attr in _STATE_ATTRS:
        monkeypatch.setattr(bridge, attr, state / getattr(bridge, attr).name)
