"""tests/ 에서 프로젝트 루트의 bridge.py 를 임포트할 수 있게 sys.path 에 루트를 추가."""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import bridge  # sys.path 주입 뒤에만 임포트 가능

# 다이제스트가 **실제로 쓰는** 상태 파일. 모듈 상수를 직접 읽는 함수(mark_seen·append_rejected·
# append_backlog·collect_awesome)를 부르는 테스트가 monkeypatch 를 빠뜨리면 라이브가 오염된다.
_STATE_ATTRS = ("SEEN_FILE", "REJECTED_FILE", "BACKLOG_FILE", "AWESOME_SNAPSHOT_FILE")


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
