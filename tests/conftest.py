"""tests/ 에서 프로젝트 루트의 bridge.py 를 임포트할 수 있게 sys.path 에 루트를 추가."""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import bridge  # sys.path 주입 뒤에만 임포트 가능

# 아래 격리 fixture 가 덮기 **전의** 실경로. 상수가 가리키는 파일이 실제로 있는지 보는
# 테스트(`test_repo_paths_actually_exist`)가 이걸 쓴다 — fixture 가 autouse 라 그냥 읽으면
# tmp 경로가 나와 **검사가 통째로 무의미해진다**(2026-08-14 실사고: `_Core/` 재배치 때
# `BACKLOG_FILE` 만 옛 경로에 남았는데 테스트 1,362건이 전부 통과했다. 쓰는 테스트가
# 하나같이 monkeypatch 해서 실경로를 아무도 안 봤기 때문).
LIVE_PATHS = {
    "BACKLOG_FILE": bridge.BACKLOG_FILE,
    "PROJECT_LABELS": bridge.REPO_ROOT / "_System" / "Core" / "project_labels.json",
    "SEEN_FILE": bridge.SEEN_FILE,
    "YT_TODAY_F": bridge.YT_TODAY_F,
    # 유튜브 산출 색인 — `BACKLOG_FILE` 과 **같은 부류의 위험**이다. 이 경로가 어긋나도
    # `append_yt_dev_log` 의 `mkdir(parents=True)` 가 엉뚱한 곳에 유령 파일을 만들며 조용히
    # 성공하고, 그 사이 `tools/yt_pick.py` 는 진짜 색인을 읽어 **중복 제거가 영구히 꺼진다**
    # (이미 다룬 영상을 계속 다시 뽑아 자막·판정 토큰을 태운다).
    "YT_DEV_LOG": bridge.YT_DEV_LOG,
}

# 다이제스트가 **실제로 쓰는** 상태 파일. 모듈 상수를 직접 읽는 함수(mark_seen·append_rejected·
# append_backlog·collect_awesome)를 부르는 테스트가 monkeypatch 를 빠뜨리면 라이브가 오염된다.
_STATE_ATTRS = (
    "SEEN_FILE",
    "REJECTED_FILE",
    "BACKLOG_FILE",
    "AWESOME_SNAPSHOT_FILE",
    # 유튜브 후보(2026-08-13) — 둘 다 라이브 파일이라 격리하지 않으면 테스트가 실물을 건드린다.
    # YT_POSTED_F 를 안 막으면 "이미 낸 스탬프"가 테스트 값으로 덮여 **실제 카드가 안 뜬다**.
    # YT_TODAY_F 는 읽기 전용이지만, 격리해야 테스트가 라이브 선별 결과에 좌우되지 않는다.
    "YT_POSTED_F",
    "YT_TODAY_F",
    # YT_DEV_LOG 도 `append_yt_dev_log` 가 **모듈 상수를 직접 읽어** 추가(append)하는 라이브
    # 파일이라 같은 성질이다 — monkeypatch 를 빠뜨린 테스트가 실색인에 가짜 행을 남기면
    # yt_pick 의 중복 제거가 그 영상을 영구히 후보에서 뺀다.
    # ⚠️ 실경로가 필요한 테스트는 `LIVE_PATHS["YT_DEV_LOG"]` 를 쓴다(여기서 tmp 로 덮인다).
    "YT_DEV_LOG",
)


# 아래 fixture 가 갈아끼우기 **전**의 실제 경로 — 임포트 시점에 잡아둔다.
# (옛 `ORIGINAL_PATHS` 는 위 LIVE_PATHS 와 목적이 같아 2026-08-14 통합했다 — 같은 일을 하는
#  dict 가 둘이면 다음 사람이 틀린 쪽에 상수를 늘린다.)


@pytest.fixture(autouse=True)
def _isolate_state_files(monkeypatch, tmp_path_factory):
    """상태 파일 4종을 **모든 테스트에서** tmp 로 돌린다(라이브 오염 방지 가드).

    2026-07-27 실제 사고: `_post_digest_cards` 를 직접 부르는 테스트가 SEEN_FILE 을 monkeypatch
    하지 않아 라이브 `logs/opensource_seen.json` 에 테스트 고정 날짜(`owner/repo-plus`:
    2026-07-15)가 기록됐다 — 그 이름이 쿨다운에 걸려 실제 후보 풀에서 빠진다.
    개별 테스트가 자기 경로로 다시 덮는 것은 자유(이 fixture 가 먼저 깔린다).
    tmp_path **밖**에 둔다 — tmp_path 를 프로젝트 루트로 쓰는 테스트(list_projects)가 있어
    거기에 폴더를 만들면 가짜 프로젝트로 잡힌다.
    """
    state = tmp_path_factory.mktemp("digest_state")
    for attr in _STATE_ATTRS:
        monkeypatch.setattr(bridge, attr, state / getattr(bridge, attr).name)


@pytest.fixture(autouse=True)
def _no_real_review(monkeypatch):
    """2차 자동 검토를 **기본 통과(passthrough)** 로 만든다 — `_isolate_state_files` 와 같은 사상.

    2026-08-02 부터 `_post_digest_cards` 가 카드 후보마다 검토 claude 를 부른다. 그 패치를
    빠뜨린 테스트는 **진짜 claude 프로세스를 띄운다**(느리고 비결정적이며 과금된다).
    검토 자체를 보는 테스트는 `bridge.review_digest_items` 를 원본으로 되돌린 뒤
    `bridge.review_repo` 만 가짜로 갈아끼운다(test_bridge.py `_REAL_REVIEW_ITEMS`).
    """
    monkeypatch.setattr(bridge, "review_digest_items", lambda items: (items, []))
