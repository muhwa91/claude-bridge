"""DiscordAdapter 계약 테스트(§5.2 — 디스코드 특화 단위).

이벤트루프 실구동(Gateway 접속)은 라이브 검증(0e) 몫이라 여기선 제외하고, 루프 없이 단위 검증
가능한 것만 다룬다: render_view(custom_id·스타일),
_message_event/_on_message 정규화·필터, _on_interaction defer 선행·custom_id 파싱·비허용 드롭,
send/edit 청킹·마스킹·버튼 말미(코루틴 경계는 _run 스텁), ack 멱등·맵 소비, close 안전성.

discord.py 미설치 환경(예: CI 최소셋)에서는 importorskip 으로 전체 스킵 → 236 코어 그린 불변.
"""

from __future__ import annotations

import asyncio
import re
import time
from types import SimpleNamespace

import pytest

discord = pytest.importorskip("discord")  # 미설치면 이 파일 전체 스킵(코어 236 은 무영향)

import bridge  # noqa: E402
import discord_adapter  # noqa: E402  (importorskip 뒤에 와야 함)
from adapter import Button, Event  # noqa: E402
from discord_adapter import DiscordAdapter, render_view  # noqa: E402

_ALLOWED = frozenset({777})


def _adapter(secrets=None, limit=discord_adapter.DISCORD_LIMIT):
    """접속하지 않는 어댑터(생성만) — poll() 을 부르지 않으면 Gateway 로 안 나간다."""
    return DiscordAdapter("tok", secrets if secrets is not None else [], _ALLOWED, limit=limit)


# ---------------------------------------------------------------------------
# render_view: Button → discord.ui.View (custom_id=액션, 스타일 매핑, ≤100자)
# ---------------------------------------------------------------------------
def test_render_view_custom_id_and_style():
    view = render_view(
        [
            Button("🧹 청소", "clean:ok", style="primary"),
            Button("✖ 취소", "clean:x", style="danger"),
            Button("🔍 판정하기", "sns_judge"),
        ]
    )
    items = view.children
    assert [it.custom_id for it in items] == ["clean:ok", "clean:x", "sns_judge"]
    assert items[0].style == discord.ButtonStyle.primary
    assert items[1].style == discord.ButtonStyle.danger
    assert items[2].style == discord.ButtonStyle.secondary  # default → 회색
    assert items[2].label == "🔍 판정하기"


def test_render_view_custom_id_within_discord_100_char_limit():
    # §1.3: DC custom_id ≤100자. 우리 액션은 전부 짧아 한도 안이고, 넘으면 잘려 parse_callback 이
    # 거르므로(오작동 대신 무시) 안전하다.
    for action in ("clean:ok", "clean:link", "clean:all", "clean:x", "sns_judge"):
        assert len(render_view([Button("L", action)]).children[0].custom_id) <= 100


# ---------------------------------------------------------------------------
# _message_event / _on_message: 수신 정규화(§1.4) + 자기·비허용 필터
# ---------------------------------------------------------------------------
def _msg(
    user_id,
    content="",
    *,
    channel_id=100,
    channel_name="trading_info",
    msg_id=5,
    atts=None,
):
    channel = SimpleNamespace(id=channel_id, name=channel_name)
    return SimpleNamespace(
        author=SimpleNamespace(id=user_id),
        channel=channel,
        content=content,
        id=msg_id,
        attachments=atts or [],
    )


def test_message_event_text_normalization():
    ev = _adapter()._message_event(_msg(777, "ㅁ노래"))
    assert ev.kind == "text"
    assert ev.channel_id == 100 and ev.user_id == 777
    assert ev.text == "ㅁ노래" and ev.message_id == 5
    assert ev.project is None and ev.channel_role is None  # 미매핑 채널


def test_message_event_dm_channel_project_none():
    channel = SimpleNamespace(id=9)  # name 속성 없음(DM)
    m = SimpleNamespace(
        author=SimpleNamespace(id=777),
        channel=channel,
        content="hi",
        id=1,
        attachments=[],
    )
    assert _adapter()._message_event(m).project is None


def test_on_message_drops_disallowed_enqueues_allowed():
    a = _adapter()  # 미접속 → client.user is None(자기 메시지 가드는 통과)
    asyncio.run(a._on_message(_msg(999, "hax")))
    assert a._queue.qsize() == 0  # 비허용 유저 드롭
    asyncio.run(a._on_message(_msg(777, "trading_info go")))
    assert a._queue.qsize() == 1
    ev = a._queue.get_nowait()
    assert ev.kind == "text" and ev.user_id == 777


def test_on_message_playlist_channel_lets_unauth_through():
    # 인가 확대: 비인가라도 플레이리스트 role 채널은 드롭 안 하고 코어로 전달(코어가 최종 판정).
    a = _adapter()
    a._channel_map = {500: ("role", "playlist")}
    asyncio.run(a._on_message(_msg(999, "ㅁ노래", channel_id=500)))
    assert a._queue.qsize() == 1
    ev = a._queue.get_nowait()
    assert ev.channel_role == "playlist" and ev.user_id == 999
    asyncio.run(a._on_message(_msg(999, "hax", channel_id=100)))  # 다른 채널 비인가 → 여전히 드롭
    assert a._queue.qsize() == 0


# ---------------------------------------------------------------------------
# _on_interaction: defer 선행(§2.3) + custom_id 파싱 + 비허용 드롭
# ---------------------------------------------------------------------------
def _interaction(user_id, custom_id, *, msg_id=42, channel_id=100, order=None):
    async def defer():
        if order is not None:
            order.append("defer")

    return SimpleNamespace(
        type=discord.InteractionType.component,
        user=SimpleNamespace(id=user_id),
        response=SimpleNamespace(defer=defer),
        data={"custom_id": custom_id},
        id=9001,
        message=SimpleNamespace(id=msg_id),
        channel_id=channel_id,
    )


def test_on_interaction_defers_before_enqueue():
    a = _adapter()
    order = []
    inter = _interaction(777, "clean:ok", order=order)

    class _RecordQueue:
        def put(self, ev):
            order.append(("put", ev))

    a._queue = _RecordQueue()
    asyncio.run(a._on_interaction(inter))
    # defer 가 큐 적재보다 반드시 먼저(§2.3 3초 규약)
    assert order[0] == "defer"
    assert order[1][0] == "put"
    ev = order[1][1]
    assert ev.kind == "button" and ev.action == "clean:ok" and ev.callback_id == "9001"
    assert ev.channel_id == 100 and ev.message_id == 42 and ev.user_id == 777
    # interaction 이 ack 용으로 맵에 등록됨
    assert a._interactions["9001"] is inter


def test_on_interaction_retired_custom_ids_become_empty_action():
    # 삭제된 옛 버튼(push·x·p:*·c:*)은 화이트리스트 밖 → action="" (코어가 ack 후 무시).
    for custom_id in ("push", "x", "p:etf_info", "c:42:1", "c:42:other"):
        a = _adapter()
        asyncio.run(a._on_interaction(_interaction(777, custom_id)))
        ev = a._queue.get_nowait()
        assert ev.action == "" and ev.action_arg == "", custom_id


def test_on_interaction_playlist_channel_lets_unauth_through():
    # 인가 확대: 비인가라도 플레이리스트 채널 버튼은 통과(코어가 clean:ok/x 만 우회 허용).
    a = _adapter()
    a._channel_map = {500: ("role", "playlist")}
    asyncio.run(a._on_interaction(_interaction(999, "clean:ok", channel_id=500)))
    assert a._queue.qsize() == 1
    ev = a._queue.get_nowait()
    assert ev.action == "clean:ok" and ev.channel_role == "playlist" and ev.user_id == 999
    a2 = _adapter()  # 다른 채널 비인가 → 드롭(defer 도 안 함)
    asyncio.run(a2._on_interaction(_interaction(999, "clean:ok", channel_id=100)))
    assert a2._queue.qsize() == 0


def test_on_interaction_sns_judge_carries_card_text_and_role():
    # 코어가 카드를 «판정 중» 으로 고치려면 누른 카드의 본문이 필요하다(Event.text).
    a = _adapter()
    a._channel_map = {300: ("role", "SNS정보")}
    inter = _interaction(777, "sns_judge", channel_id=300, msg_id=77)
    inter.message = SimpleNamespace(id=77, content="📥 SNS 1건 수집함 저장")
    asyncio.run(a._on_interaction(inter))
    ev = a._queue.get_nowait()
    assert (ev.action, ev.action_arg, ev.message_id) == ("sns_judge", "", 77)
    assert ev.text == "📥 SNS 1건 수집함 저장" and ev.channel_role == "SNS정보"


def test_history_after_reads_oldest_first_after_id_and_normalizes():
    a = _adapter()
    a._channel_map = {300: ("role", "SNS정보")}
    seen = {}

    async def history(**kw):
        seen.update(kw)
        for m in (
            _msg(777, "https://x.com/a/status/1", channel_id=300, msg_id=11),
            _msg(1, "봇 카드", channel_id=300, msg_id=12),
        ):
            yield m

    channel = SimpleNamespace(history=history)
    a._client = SimpleNamespace(get_channel=lambda _cid: channel)  # type: ignore[assignment]
    events = asyncio.run(a._history_coro(300, 10, 1000))
    # 상한을 discord.py 에 그대로 넘긴다(무제한 limit=None 금지 — 폭주 대비)
    assert seen["after"].id == 10 and seen["oldest_first"] is True and seen["limit"] == 1000
    assert [(e.message_id, e.user_id, e.channel_role) for e in events] == [
        (11, 777, "SNS정보"),
        (12, 1, "SNS정보"),  # 작성자 필터는 코어 몫 — 어댑터는 그대로 돌려준다
    ]


def test_history_after_failure_is_none():
    a = _adapter()  # 루프 미준비 → _run 이 None
    assert a.history_after(300, 10, 1000) is None


def test_on_interaction_unknown_custom_id_becomes_empty_action():
    a = _adapter()
    asyncio.run(a._on_interaction(_interaction(777, "bogus")))
    ev = a._queue.get_nowait()
    assert ev.action == "" and ev.action_arg == ""  # 코어가 ack 후 무시


def test_on_interaction_disallowed_user_dropped_no_defer():
    a = _adapter()
    order = []
    inter = _interaction(999, "push", order=order)
    asyncio.run(a._on_interaction(inter))
    assert order == []  # defer 조차 안 함
    assert a._queue.qsize() == 0
    assert a._interactions == {}


def test_on_interaction_ignores_non_component():
    a = _adapter()
    inter = _interaction(777, "push")
    inter.type = discord.InteractionType.application_command
    asyncio.run(a._on_interaction(inter))
    assert a._queue.qsize() == 0


# ---------------------------------------------------------------------------
# send / edit: 청킹·마스킹·버튼 말미 (_run·coro 스텁으로 루프 없이 검증)
# ---------------------------------------------------------------------------
def _stub_calls(adapter, ids):
    """_send_coro/_edit_coro 를 튜플로, _run 을 레코더로 대체(코루틴 미생성 → 경고 없음)."""
    calls = []
    adapter._send_coro = lambda cid, body, view: ("send", cid, body, view)  # type: ignore[assignment]
    adapter._edit_coro = lambda cid, mid, body, view: ("edit", cid, mid, body, view)  # type: ignore[assignment]
    it = iter(ids)

    def fake_run(coro):
        calls.append(coro)
        return next(it, None)

    adapter._run = fake_run  # type: ignore[assignment]
    return calls


def test_send_single_chunk_returns_first_id():
    a = _adapter()
    calls = _stub_calls(a, [111])
    mid = a.send(100, "짧은 응답")
    assert mid == 111
    assert len(calls) == 1
    assert calls[0] == ("send", 100, "짧은 응답", None)


def test_send_masks_secrets():
    a = _adapter(secrets=["SECRET"])
    calls = _stub_calls(a, [1])
    a.send(100, "token=SECRET 노출")
    assert calls[0][2] == "token=*** 노출"


def test_render_parts_empty_body_uses_placeholder():
    # 디스코드는 빈 content 를 400 거부 → _render_parts 가 "(빈 응답)" 로 방어.
    assert _adapter()._render_parts("") == ["(빈 응답)"]


def test_send_chunks_buttons_on_last_only():
    a = _adapter(limit=5)
    calls = _stub_calls(a, [10, 20, 30])
    mid = a.send(100, "abcdefghijkl", [Button("Push", "push", style="primary")])  # 12자 → 3청크
    assert mid == 10  # 첫 청크 id
    assert len(calls) == 3
    # 버튼(view)은 마지막 청크에만
    assert calls[0][3] is None and calls[1][3] is None
    assert calls[2][3] is not None  # render_view 결과(View)


def test_edit_overflow_edits_head_then_sends_rest():
    a = _adapter(limit=5)
    calls = _stub_calls(a, [None, None, None])
    a.edit(100, 42, "abcdefghijkl", [Button("Push", "push")])  # 3청크
    # edit 튜플=(edit,cid,mid,body,view)→view=[4]. send 튜플=(send,cid,body,view)→view=[3].
    assert calls[0][0] == "edit" and calls[0][1] == 100 and calls[0][2] == 42
    assert calls[0][4] is None  # head 는 다청크라 버튼 없음
    assert calls[1][0] == "send" and calls[2][0] == "send"
    assert calls[2][3] is not None  # 마지막 후속 발행에 버튼


def test_edit_single_chunk_keeps_buttons_on_head():
    a = _adapter()
    calls = _stub_calls(a, [None])
    a.edit(100, 42, "짧음", [Button("Push", "push")])
    assert len(calls) == 1
    assert calls[0][0] == "edit"
    assert calls[0][4] is not None  # 단일 청크 → head 에 버튼


# ---------------------------------------------------------------------------
# ack: 멱등·맵 소비 / close: 안전성
# ---------------------------------------------------------------------------
def test_ack_none_callback_is_noop():
    a = _adapter()
    ran = []
    a._run = lambda coro: ran.append(coro)  # type: ignore[assignment]
    a.ack(None)
    a.ack("")
    assert ran == []


def test_ack_consumes_map_and_is_idempotent():
    a = _adapter()
    a._interactions["9001"] = SimpleNamespace()
    a.ack("9001")  # 이미 defer 됨 → 응답할 것 없음, 맵만 정리
    assert "9001" not in a._interactions
    a.ack("9001")  # 재호출·미등록도 무해
    a.ack("nope")


def test_close_before_start_is_safe_and_sets_sentinel():
    a = _adapter()
    a.close()  # 스레드·루프 미기동 상태에서도 예외 없이
    assert a._closed is True
    assert a._queue.get_nowait() is None  # poll 해제용 종료 센티넬


def test_run_bot_login_failure_signals_poll_sentinel():
    # L-1: 잘못된 토큰(LoginFailure)으로 봇 스레드가 죽으면 poll 이 queue.get() 에서 영구 블록된다 —
    # _run_bot 이 예외를 포착하고 종료 센티넬을 큐에 넣어 poll·main 이 깨끗이 끝나게 한다.
    a = _adapter()

    async def _boom():
        raise discord.LoginFailure("bad token")

    async def _aclose():
        return None

    a._client.start = lambda *_a, **_k: _boom()  # type: ignore[assignment]
    a._client.close = lambda *_a, **_k: _aclose()  # type: ignore[assignment]
    a._run_bot()  # 동기 실행 — 예외를 삼키고 센티넬을 넣어야 함(무한 블록 방지)
    assert a._closed is True
    assert a._queue.get_nowait() is None  # poll 해제 센티넬(봇 사망 시)


def test_poll_terminates_when_bot_thread_dies(monkeypatch):
    # L-1 통합: 봇 스레드가 죽어 센티넬만 들어오면 poll 은 정상 종료(무한 블록 X).
    a = _adapter()
    monkeypatch.setattr(a, "_start", lambda: None)  # 실제 Gateway 접속 방지
    a._queue.put(None)  # 봇 사망 시 _run_bot 이 넣는 센티넬을 모사
    assert list(a.poll()) == []  # 블록 없이 즉시 종료


def test_poll_drains_queue_until_sentinel(monkeypatch):
    a = _adapter()
    monkeypatch.setattr(a, "_start", lambda: None)  # Gateway 접속 방지
    ev = Event(kind="text", channel_id=1, user_id=777, text="hi")
    a._queue.put(ev)
    a._queue.put(None)  # 센티넬
    got = list(a.poll())
    assert got == [ev]


def test_run_without_loop_returns_none_and_closes_coro():
    a = _adapter()  # _loop 은 None(미기동)

    async def coro():
        return 1

    c = coro()
    assert a._run(c) is None  # 루프 미준비 → None
    # 코루틴이 close 돼 "never awaited" 경고가 안 남(파괴 시점 검증은 생략, 호출만으로 close 됨)


# ---------------------------------------------------------------------------
# §4.1 상태색 임베드 렌더 — text 헤더 판정(계약 무변경) / _style success·secondary
# ---------------------------------------------------------------------------


def test_send_plain_stays_content_str():
    a = _adapter()
    calls = _stub_calls(a, [1])
    a.send(100, "대상 프로젝트 2")
    assert calls[0][2] == "대상 프로젝트 2"  # plain 그대로(str)


def test_wait_ready_reflects_on_ready_event():
    # 재시작 복귀 통지: on_ready 전엔 False(대기), set 후 True(접속 완료 → send 안전).
    a = _adapter()
    assert a.wait_ready(0.01) is False
    a._ready.set()  # on_ready 모사
    assert a.wait_ready(0.01) is True


# ---------------------------------------------------------------------------
# ①(채널 자동생성 §4.4) — channel_map 영속·역조회·라우팅 채우기·자동생성(mock guild)
# ---------------------------------------------------------------------------


def test_channel_map_roundtrip(tmp_path):
    p = tmp_path / "cm.json"
    m = {10: ("role", "데이터분석"), 20: ("project", "etf_info")}
    discord_adapter.save_channel_map(p, m)
    assert discord_adapter.load_channel_map(p) == m


def test_load_channel_map_missing_and_corrupt(tmp_path):
    assert discord_adapter.load_channel_map(tmp_path / "none.json") == {}
    p = tmp_path / "bad.json"
    p.write_text("{bad", encoding="utf-8")
    assert discord_adapter.load_channel_map(p) == {}


def test_role_channel_reverse_lookup():
    a = _adapter()
    a._channel_map = {
        10: ("role", "간단처리"),
        20: ("project", "etf_info"),
        30: ("role", "봇상태"),
    }
    assert a.role_channel("간단처리") == 10
    assert a.role_channel("봇상태") == 30
    assert a.role_channel("없는역할") is None


# ---------------------------------------------------------------------------
# clear_channel / _purge_coro — 채널 메시지 전체 삭제(파괴적, 부분 성공 허용)
# ---------------------------------------------------------------------------
def test_clear_channel_delegates_to_run_and_defaults_zero():
    a = _adapter()
    a._run = lambda coro, **_kw: (coro.close(), 7)[1]  # type: ignore[assignment]  # 닫고 카운트
    assert a.clear_channel(100) == 7
    a._run = lambda coro, **_kw: (coro.close(), None)[1]  # type: ignore[assignment]  # 미준비 폴백
    assert a.clear_channel(100) == 0


def test_purge_coro_loops_until_exhausted():
    a = _adapter()
    batches = [[object()] * 100, [object()] * 30]  # 100 → 30(<100)에서 종료
    calls = []

    class _Ch:
        async def purge(self, *, limit, bulk):
            calls.append((limit, bulk))
            return batches[len(calls) - 1] if len(calls) <= len(batches) else []

    a._client.get_channel = lambda _cid: _Ch()  # type: ignore[assignment]
    assert asyncio.run(a._purge_coro(100)) == 130
    assert len(calls) == 2 and calls[0] == (100, True)


def test_purge_coro_bounded_range_after_and_upto_inclusive():
    # #SNS정보 청소 = 수집기가 본 범위만: (after_id, upto_id] → purge(after=, before=upto_id+1)
    a = _adapter()
    seen = []

    class _Ch:
        async def purge(self, **kw):
            seen.append(kw)
            return [object()] * 3

    a._client.get_channel = lambda _cid: _Ch()  # type: ignore[assignment]
    assert asyncio.run(a._purge_coro(100, after_id=10, upto_id=20)) == 3
    [kw] = seen
    assert kw["after"].id == 10 and kw["before"].id == 21 and kw["limit"] == 100
    seen.clear()
    asyncio.run(a._purge_coro(100))  # 경계 없음 = 종전(전체) — before/after 를 넘기지 않는다
    assert "after" not in seen[0] and "before" not in seen[0]


def test_purge_coro_keep_uses_check_and_scans_whole_range():
    # keep 이 있으면 purge(check=) 한 번 — limit 는 «훑은 수» 라 None(범위로 유계), 반복하지 않는다
    a = _adapter()
    seen = []
    msgs = [
        SimpleNamespace(content="https://youtu.be/x", author=SimpleNamespace(id=1)),
        SimpleNamespace(content="메모", author=SimpleNamespace(id=1)),
        SimpleNamespace(content=None, author=SimpleNamespace(id=2)),
    ]

    class _Ch:
        async def purge(self, **kw):
            seen.append(kw)
            return [m for m in msgs if kw["check"](m)]

    a._client.get_channel = lambda _cid: _Ch()  # type: ignore[assignment]
    kept = []

    def keep(text, author):
        kept.append((text, author))
        return "://" in text

    assert asyncio.run(a._purge_coro(100, after_id=1, upto_id=9, keep=keep)) == 2
    [kw] = seen
    assert kw["limit"] is None and kw["bulk"] is True
    assert kw["after"].id == 1 and kw["before"].id == 10
    assert kept == [("https://youtu.be/x", 1), ("메모", 1), ("", 2)]  # 본문 없음 = ""


def test_delete_message_deletes_partial_message_and_swallows_failure():
    a = _adapter()
    seen = []

    class _Msg:
        async def delete(self):
            seen.append("deleted")

    class _Ch:
        def get_partial_message(self, mid):
            seen.append(mid)
            return _Msg()

    a._client.get_channel = lambda _cid: _Ch()  # type: ignore[assignment]
    asyncio.run(a._delete_message_coro(1, 77))
    assert seen == [77, "deleted"]
    a._run = lambda coro, **_kw: (coro.close(), None)[1]  # type: ignore[assignment]
    assert a.delete_message(1, 77) is None  # 실패·미준비는 로그만(계약)


def test_clear_channel_passes_bounds_to_purge():
    a = _adapter()
    got = {}

    async def fake_purge(channel_id, **kw):
        got.update(kw, channel_id=channel_id)
        return 0

    a._purge_coro = fake_purge  # type: ignore[assignment]
    timeouts = []
    a._run = lambda coro, timeout=None: (timeouts.append(timeout), asyncio.run(coro))[1]  # type: ignore[assignment]
    a.clear_channel(5, after_id=1, upto_id=9)
    assert got == {"channel_id": 5, "after_id": 1, "upto_id": 9, "keep": None}
    # 14일 넘은 메시지 개별 삭제가 수 분 걸릴 수 있다 — 청소 전용 긴 타임아웃(바쁨 먼저 안 풀리게)
    assert timeouts == [discord_adapter._PURGE_TIMEOUT]
    assert discord_adapter._PURGE_TIMEOUT >= 600


def test_purge_coro_partial_failure_returns_deleted_count():
    a = _adapter()

    class _Ch:
        def __init__(self):
            self.n = 0

        async def purge(self, **_kw):
            self.n += 1
            if self.n == 1:
                return [object()] * 100
            raise discord.DiscordException("boom")  # 권한 없음·API 오류 모사

    a._client.get_channel = lambda _cid: _Ch()  # type: ignore[assignment]
    assert asyncio.run(a._purge_coro(100)) == 100  # 부분 성공: 삭제된 만큼만


def test_enqueue_coro_fifo_after_current_index():
    # 재생 중 연속 추가 = FIFO — 현재 곡 뒤에 추가순서대로 쌓인다(A→B→C).
    a = _adapter()
    a._voice = SimpleNamespace(is_connected=lambda: True)
    a._music_entries = [{"id": f"cur{i}"} for i in range(5)]
    a._music_index = 2
    assert asyncio.run(a._enqueue_coro("A", "가")) == 6  # 편입 후 큐 곡수
    assert asyncio.run(a._enqueue_coro("B", "나")) == 7
    assert asyncio.run(a._enqueue_coro("C", "다")) == 8
    # 현재곡(idx2) 뒤에 A,B,C 순 — index+1 고정 아님(그건 LIFO), queued 블록 뒤로 이어붙임.
    assert [e["id"] for e in a._music_entries[3:6]] == ["A", "B", "C"]
    assert all(a._music_entries[i].get("queued") for i in (3, 4, 5))
    assert a._music_entries[3] == {"id": "A", "title": "가", "queued": True}  # 소비 형식


def test_reshuffle_clears_queued_markers():
    # 재셔플은 이미 재생된 추가곡의 queued 마커를 지워, 다음 바퀴 삽입 스캔이 스테일에 안 걸린다.
    a = _adapter()
    a._music_entries = [{"id": "x", "queued": True}, {"id": "y"}, {"id": "z", "queued": True}]
    a._reshuffle_entries()
    assert all("queued" not in e for e in a._music_entries)  # 전부 제거
    assert {e["id"] for e in a._music_entries} == {"x", "y", "z"}  # 곡은 보존(개수·구성 불변)


def test_enqueue_coro_fifo_after_playback_progressed():
    # 재생이 진행돼 index 가 대기 블록을 지나가면, 새 추가는 남은 큐 뒤(현재 index+1)에 붙는다.
    a = _adapter()
    a._voice = SimpleNamespace(is_connected=lambda: True)
    a._music_entries = [{"id": "cur"}, {"id": "A", "queued": True}, {"id": "orig"}]
    a._music_index = 1  # A 재생 중(지나감) — 뒤의 orig 는 queued 아님
    assert asyncio.run(a._enqueue_coro("D", "라")) == 4
    assert [e["id"] for e in a._music_entries] == ["cur", "A", "D", "orig"]  # index+1 에 삽입


class _FakeVoice:
    """discord.py VoiceClient 의 **실동작**을 최소로 흉내낸다.

    🔴 `stop()` 을 «항상 먹는 것» 으로 만들면 A1 같은 결함군을 원리적으로 못 잡는다 — 실제로는
    재생 중이 아니면 **no-op 이고 _after 도 안 뜬다**. 그 의미를 여기 반영해 둔다.
    (play() 의 중복 재생 예외까지는 흉내내지 않는다 — 테스트는 _advance 를 직접 태운다.)
    """

    def __init__(self, played):
        self._played = played
        self.playing = False
        self.stops = 0  # **먹은** stop() 횟수(미재생 no-op 은 세지 않는다)
        self.connected = True

    def is_connected(self):
        return self.connected

    def play(self, src, after=None):
        self.playing = True
        self._played.append((src, after))

    def stop(self):
        if not self.playing:
            return  # 미재생 → no-op(_after 가 안 뜬다)
        self.playing = False
        self.stops += 1

    async def disconnect(self, **_kw):
        self.connected = False


def _playing(monkeypatch, entries, index=0, *, text_ch=500):
    """`_play_current`·`_advance` 를 **실제로 태울 수 있는** 어댑터 + (재생 기록, 발송 기록).

    무력화하는 것은 재생 그 자체(yt-dlp 스트림 해석·ffmpeg·디스코드 전송)뿐이라, 알림 배선과
    인덱스 이동은 진짜 코드가 돈다 — 손으로 `+1` 을 흉내내면 _advance 의 wrap·재셔플 분기
    변경을 못 잡는다(리뷰 2).
    """
    a = _adapter()
    a._music_entries = [dict(e) for e in entries]
    a._music_index = index
    a._music_text_ch = text_ch
    a._extract_stream = lambda _entry: "http://stream"  # type: ignore[method-assign]
    a._ffmpeg_log_off = True  # 테스트가 logs/ffmpeg.log 를 만들지 않게(핸들 검증은 전용 테스트)
    monkeypatch.setattr(discord, "FFmpegPCMAudio", lambda *_a, **_k: object())
    played: list[object] = []
    a._voice = _FakeVoice(played)
    sent: list[tuple[int, str]] = []

    async def fake_send_coro(cid, payload, _view):
        sent.append((cid, payload))
        return 1

    a._send_coro = fake_send_coro  # type: ignore[method-assign]
    return a, played, sent


def test_play_current_announces_title(monkeypatch):
    # 🔴 요구사항 ①의 유일한 배선 단언 — _play_current 의 알림을 지우면 이 테스트가 깨진다.
    a, played, sent = _playing(monkeypatch, [{"id": "v1", "title": "밤편지"}])
    asyncio.run(a._play_current())
    assert len(played) == 1  # 재생은 시작됐고
    assert sent == [(500, "💿 현재 재생 곡\n`밤편지`")]  # 머리글 + 제목 2줄이 채널로 나갔다


def test_play_current_announce_is_two_lines_and_cleaned(monkeypatch):
    """알림 = '💿 현재 재생 곡' + 개행 + clean_track_title 결과(2026-08-18 운영자 요청 형식)."""
    a, _played, sent = _playing(
        monkeypatch, [{"id": "v1", "title": "오반 (OVAN) - 행복 Happiness [Music Video]"}]
    )
    asyncio.run(a._play_current())
    assert sent == [(500, f"{discord_adapter.MUSIC_NOW_HEADER}\n`오반 - 행복`")]
    head, _, body = sent[0][1].partition("\n")
    assert head == f"{discord_adapter.MUSIC_NOW_EMOJI} 현재 재생 곡" and body == "`오반 - 행복`"
    # 정리는 **표시 전용** — 엔트리의 원본 제목은 그대로여야 'ㅁ삭제'·'ㅁ목록' 매칭이 산다.
    assert a._music_entries[0]["title"] == "오반 (OVAN) - 행복 Happiness [Music Video]"


def test_play_current_fills_artist_only_for_topic_channel(monkeypatch):
    """🔴 곡 알림의 가수 채우기는 **Topic 채널일 때만** — 넓히면 틀린 가수가 곡마다 나간다.

    실측(재생목록 101곡, 현행 코드 기준): 가수가 안 붙는 21곡 중 Topic 은 2곡뿐이고 나머지
    19곡은 가사채널·팬업로드다.
    """
    a, _p, sent = _playing(
        monkeypatch, [{"id": "v1", "title": "사랑하니까", "channel": "더 크로스 - Topic"}]
    )
    asyncio.run(a._play_current())
    assert sent == [(500, "💿 현재 재생 곡\n`더 크로스 - 사랑하니까`")]
    # 비-Topic 채널(가사채널)은 채널명이 가수가 아니다 → 정리된 제목만.
    a2, _p2, sent2 = _playing(
        monkeypatch, [{"id": "v2", "title": "Magical Syndrome", "channel": "글집"}]
    )
    asyncio.run(a2._play_current())
    assert sent2 == [(500, "💿 현재 재생 곡\n`Magical Syndrome`")]
    # channel 키가 없는 엔트리(큐 편입분)는 폴백 — 정리된 제목만.
    a3, _p3, sent3 = _playing(monkeypatch, [{"id": "v3", "title": "사랑하니까"}])
    asyncio.run(a3._play_current())
    assert sent3 == [(500, "💿 현재 재생 곡\n`사랑하니까`")]
    # 🔴 공백뿐인 제목 → display_title 이 '' 를 줘야 `or entry["id"]` 폴백이 산다(종전엔 공백을
    # 그대로 돌려줘 truthy 로 통과, `X -    ` 가 알림으로 나갔다).
    a4, _p4, sent4 = _playing(
        monkeypatch, [{"id": "v4", "title": "   ", "channel": "가수 - Topic"}]
    )
    asyncio.run(a4._play_current())
    assert sent4 == [(500, "💿 현재 재생 곡\n`v4`")]


def test_play_current_announce_header_only_when_no_title_and_no_id(monkeypatch):
    # 제목도 id 도 없으면 빈 둘째 줄을 붙이지 않고 머리글만 보낸다.
    a, _played, sent = _playing(monkeypatch, [{"title": ""}])
    asyncio.run(a._play_current())
    assert sent == [(500, discord_adapter.MUSIC_NOW_HEADER)]


def test_play_current_rechecks_voice_after_await(monkeypatch):
    """A1: 스트림 해석(await, 실사용 1~3초) 사이에 ㅁ정지가 들어와도 죽지 않아야 한다.

    루프 머리의 `not self._music_stopping` 검사는 이 await **이전**에 끝난다. 재검사가 없으면
    _music_stop_coro 가 만든 `_voice = None` 위로 play() 를 쳐서 AttributeError 가 나고,
    _advance 는 맨 태스크라 아무도 그 예외를 받지 않는다("Task exception was never retrieved").
    """
    a, played, _sent = _playing(monkeypatch, [{"id": "v1", "title": "곡1"}])
    a._extract_stream = lambda _e: (time.sleep(0.05), "http://stream")[1]  # 끼어들 창을 연다

    async def scenario():
        task = asyncio.create_task(a._play_current())
        await asyncio.sleep(0.01)  # 추출이 실행기 스레드에서 도는 동안
        await a._music_stop_coro()  # 그 창에서 ㅁ정지 — _voice 를 None 으로 만든다
        await task  # 예외가 나면 여기서 터진다(= 회귀)

    asyncio.run(scenario())
    assert played == []  # 정지된 뒤라 play() 는 호출되지 않았다
    assert a._voice is None


def test_play_current_gives_up_after_one_full_round(monkeypatch):
    """A2: 전곡 추출 실패 시 무한 스핀하지 않고 빠져나오며 이유를 1회 알린다.

    탈출 조건이 없으면 2초에 18,737회 돌며 회전 없는 bridge.log 를 채운다(실측).
    """
    a, played, sent = _playing(monkeypatch, [{"id": f"v{i}", "title": f"곡{i}"} for i in range(3)])

    def boom(_entry):
        raise RuntimeError("403")

    a._extract_stream = boom

    async def bounded():
        await asyncio.wait_for(a._play_current(), 2.0)  # 무한 스핀이면 TimeoutError

    asyncio.run(bounded())
    assert played == []
    assert sent == [(500, "⚠️ 재생 가능 목록 없음")]  # 무음의 이유를 알 수 있게


def test_extract_stream_avoids_broken_dash_clients(monkeypatch):
    """재생 403 회귀 — 오디오 전용(opus 251)을 주는 클라이언트가 목록에 있으면 안 된다.

    셋(android_vr·web_embedded·tv_downgraded) 다 통짜 GET 이 403 이라 ffmpeg 이 90ms 만에 죽는다.
    ⚠️ «폴백» 으로 하나만 남겨도 소용없다 — bestaudio 는 클라이언트 순서가 아니라 오디오 품질로
    골라서 고장난 251 을 다시 뽑는다. 네트워크는 타지 않는다(yt-dlp 를 통째로 모킹).
    """
    seen: dict = {}

    class FakeYDL:
        def __init__(self, opts):
            seen.update(opts)

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def extract_info(self, ref, **_kw):
            return {"url": f"http://stream/{ref}"}

    monkeypatch.setattr(discord_adapter.yt_dlp, "YoutubeDL", FakeYDL)
    assert _adapter()._extract_stream({"id": "vid1"}) == "http://stream/vid1"
    clients = seen["extractor_args"]["youtube"]["player_client"]
    assert clients == ["android", "tv_simply"]
    assert not ({"android_vr", "web_embedded", "tv_downgraded"} & set(clients))


def test_search_candidates_returns_five_with_channel(monkeypatch):
    """검색은 ytsearch5 **1회**·flat 추출(full 은 후보당 ~1초). 채널명은 코어 필터의 입력이다."""
    seen: dict = {}
    refs: list[str] = []

    class FakeYDL:
        def __init__(self, opts):
            seen.update(opts)

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def extract_info(self, ref, **_kw):
            refs.append(ref)
            return {
                "entries": [
                    {"id": "v1", "title": "무대 직캠", "channel": "Mnet"},
                    {"id": "v2", "title": "[가사] 곡", "uploader": "1theK"},  # uploader 폴백
                    {"id": None, "title": "id 없음"},  # 건너뛴다
                    "쓰레기",  # dict 아님 — 건너뛴다
                ]
            }

    monkeypatch.setattr(discord_adapter.yt_dlp, "YoutubeDL", FakeYDL)
    a = _adapter()
    assert a.search_candidates("곡") == [("v1", "무대 직캠", "Mnet"), ("v2", "[가사] 곡", "1theK")]
    assert refs == ["ytsearch5:곡"] and seen["extract_flat"] == "in_playlist"
    # search_video 는 코어 필터(pick_index)를 태운다 — 방송무대 1위를 건너뛴다.
    assert a.search_video("곡") == ("v2", "[가사] 곡")
    assert a.search_video("곡", 1) == ("v1", "무대 직캠")  # '#N' 은 필터 무시
    assert a.search_video("곡", 9) is None  # 범위 밖


def test_search_candidates_failure_returns_empty(monkeypatch):
    class BoomYDL:
        def __init__(self, _opts):
            raise RuntimeError("yt-dlp 폭발")

    monkeypatch.setattr(discord_adapter.yt_dlp, "YoutubeDL", BoomYDL)
    assert _adapter().search_candidates("곡") == []
    assert _adapter().search_video("곡") is None


def test_play_current_passes_stderr_to_ffmpeg(monkeypatch, tmp_path):
    """🔴 stderr 를 안 넘기면 ffmpeg 의 403 이 통째로 사라진다(3주간 결함이 숨은 이유).

    핸들은 어댑터 수명에 1회만 연다 — 곡마다 열지 않는다.
    """
    a, _played, _sent = _playing(monkeypatch, [{"id": "v1", "title": "곡1"}, {"id": "v2"}])
    monkeypatch.setattr(discord_adapter, "FFMPEG_LOG_FILE", tmp_path / "logs" / "ffmpeg.log")
    a._ffmpeg_log_off = False  # _playing 이 껐던 것을 이 테스트에서만 켠다
    handles = []

    def fake_ffmpeg(*_a, **kw):
        handles.append(kw.get("stderr"))
        return object()

    monkeypatch.setattr(discord, "FFmpegPCMAudio", fake_ffmpeg)
    asyncio.run(a._play_current())
    asyncio.run(a._advance())
    assert handles[0] is not None and handles[0] is handles[1]  # 같은 핸들 재사용(곡마다 열지 않음)
    assert (tmp_path / "logs" / "ffmpeg.log").exists()  # logs/ 는 없으면 만든다
    a.close()
    assert handles[0].closed  # close() 가 핸들을 정리한다


def test_ffmpeg_stderr_opens_unbuffered_append(monkeypatch, tmp_path):
    # 🔴 buffering=0 이 이 기능의 핵심 — 봇은 상시 프로세스라 flush 시점이 오지 않는다.
    # 버퍼링이 켜지면 403 이 디스크에 안 닿아 stderr 를 넘긴 의미가 통째로 사라진다.
    seen: dict = {}
    real = tmp_path / "ffmpeg.log"

    class FakePath:
        parent = SimpleNamespace(mkdir=lambda **_kw: None)

        def open(self, mode, **kw):
            seen["mode"] = mode
            seen.update(kw)
            return real.open("ab", buffering=0)

    monkeypatch.setattr(discord_adapter, "FFMPEG_LOG_FILE", FakePath())
    a = _adapter()
    assert a._ffmpeg_stderr() is not None
    assert seen == {"mode": "ab", "buffering": 0}  # append + 무버퍼
    a.close()


def test_ffmpeg_stderr_open_failure_does_not_block_playback(monkeypatch, tmp_path):
    # 로그를 못 열어도(권한·경로) 재생은 계속돼야 한다 — stderr=None 으로 진행, 재시도는 안 한다.
    a = _adapter()
    blocker = tmp_path / "blocker"
    blocker.write_text("x")  # 파일 밑에는 폴더를 못 만든다 → mkdir 이 OSError
    monkeypatch.setattr(discord_adapter, "FFMPEG_LOG_FILE", blocker / "logs" / "ffmpeg.log")
    assert a._ffmpeg_stderr() is None
    assert a._ffmpeg_log_off is True  # 곡마다 경고가 도배되지 않게 한 번만 시도
    a.close()  # 핸들이 없어도 안전


def test_advance_plays_next_and_announces(monkeypatch):
    # 실제 _advance 를 태운다(손으로 +1 흉내내지 않는다).
    a, _played, sent = _playing(
        monkeypatch, [{"id": "a", "title": "A"}, {"id": "b", "title": "B"}], index=0
    )
    asyncio.run(a._advance())
    assert a._music_index == 1 and sent == [(500, "💿 현재 재생 곡\n`B`")]


def test_advance_wraps_and_reshuffles(monkeypatch):
    # 마지막 곡에서 넘어가면 재셔플 + 처음부터(무한 반복). 재셔플이 queued 마커도 지운다.
    a, _played, sent = _playing(
        monkeypatch,
        [{"id": "a", "title": "A", "queued": True}, {"id": "b", "title": "B"}],
        index=1,
    )
    asyncio.run(a._advance())
    assert a._music_index == 0
    assert all("queued" not in e for e in a._music_entries)
    assert len(sent) == 1 and sent[0][1].startswith("💿 현재 재생 곡\n")


def test_notify_music_sends_via_send_coro():
    # 음악 알림은 _send_coro 를 직접 await 한다 — self.send() 는 _run 동기대기라 루프 위에서
    # 부르면 데드락(§3.2). 채널 미상(0)이면 건너뛰고, 전송 실패는 삼킨다(재생이 끊기면 안 된다).
    a = _adapter()
    sent = []

    async def fake_send_coro(cid, payload, view):
        sent.append((cid, payload, view))
        return 1

    a._send_coro = fake_send_coro  # type: ignore[method-assign]
    a._music_text_ch = 0
    asyncio.run(a._notify_music("💿 현재 재생 곡\n밤편지"))
    assert sent == []  # 재생 채널 미상 → 스킵
    a._music_text_ch = 500
    asyncio.run(a._notify_music("💿 현재 재생 곡\n밤편지"))
    assert sent == [(500, "💿 현재 재생 곡\n밤편지", None)]

    async def boom(_cid, _payload, _view):
        raise discord.DiscordException("권한 없음")

    a._send_coro = boom  # type: ignore[method-assign]
    asyncio.run(a._notify_music("💿 현재 재생 곡\n곡"))  # 예외 전파 없음(재생 계속)


def test_play_current_falls_back_to_id_when_title_missing(monkeypatch):
    # 알림 제목은 title, 없으면 id.
    a, _played, sent = _playing(monkeypatch, [{"id": "v2"}])
    asyncio.run(a._play_current())
    assert sent == [(500, "💿 현재 재생 곡\n`v2`")]


def test_dequeue_coro_shifts_index_when_removing_before_current():
    # 'ㅁ삭제'로 현재 곡 **앞**의 곡을 빼면 _music_index 를 그만큼 당겨야 한다 —
    # 안 당기면 재생 중인 곡이 바뀐 것처럼 밀려 다음 곡이 어긋난다(인덱스 보정 회귀).
    a = _adapter()
    a._voice = SimpleNamespace(is_connected=lambda: True, stop=lambda: pytest.fail("현재곡 아님"))
    a._music_entries = [{"id": "x"}, {"id": "dup"}, {"id": "cur"}, {"id": "dup"}, {"id": "y"}]
    a._music_index = 2  # cur 재생 중
    assert asyncio.run(a._dequeue_coro("dup")) == 2  # 앞뒤로 1건씩 제거
    assert [e["id"] for e in a._music_entries] == ["x", "cur", "y"]
    assert a._music_entries[a._music_index]["id"] == "cur"  # 여전히 같은 곡을 가리킨다


def test_dequeue_coro_skips_to_next_when_removing_current(monkeypatch):
    # 현재 곡을 지우면 stop() → (실제)_advance 가 '지운 곡의 다음 곡'을 튼다(한 곡 건너뜀 방지).
    a, _played, sent = _playing(
        monkeypatch,
        [{"id": "a", "title": "A"}, {"id": "cur", "title": "현재"}, {"id": "b", "title": "B"}],
        index=1,
    )
    assert asyncio.run(a._dequeue_coro("cur")) == 1
    assert [e["id"] for e in a._music_entries] == ["a", "b"]
    asyncio.run(a._advance())  # _after 가 하는 일을 실제로 태운다
    assert sent == [(500, "💿 현재 재생 곡\n`B`")]


def test_dequeue_current_at_index_zero_then_advance(monkeypatch):
    # 현재 곡이 맨 앞이면 보정 후 _music_index 가 -1 이 된다 — 도달하는 경로가 _advance(선 +1)
    # 뿐이라 entries[-1] 을 읽는 일이 없다(전수 검증 확인분). 실제로 태워 다음 곡이 맞는지 본다.
    a, _played, sent = _playing(
        monkeypatch,
        [{"id": "cur", "title": "현재"}, {"id": "b", "title": "B"}],
        index=0,
    )
    assert asyncio.run(a._dequeue_coro("cur")) == 1
    assert a._music_index == -1
    asyncio.run(a._advance())
    assert a._music_index == 0 and sent == [(500, "💿 현재 재생 곡\n`B`")]


def test_dequeue_last_entry_wraps_to_reshuffle(monkeypatch):
    # 목록 **마지막** 곡을 재생 중에 지우면 다음이 없다 → _advance 가 wrap 해 재셔플·처음부터.
    a, _played, sent = _playing(
        monkeypatch,
        [{"id": "a", "title": "A"}, {"id": "b", "title": "B"}, {"id": "cur", "title": "현재"}],
        index=2,
    )
    assert asyncio.run(a._dequeue_coro("cur")) == 1
    assert a._music_index == 1  # 남은 [A,B] 의 마지막 → 다음 +1 이 곧 wrap
    asyncio.run(a._advance())
    assert a._music_index == 0 and len(sent) == 1
    # 재셔플이라 순서는 무작위
    assert sent[0][1] in ("💿 현재 재생 곡\n`A`", "💿 현재 재생 곡\n`B`")


def test_dequeue_coro_noop_when_queue_would_empty():
    # 전부 지우면 재생이 끊긴다 → 손대지 않고 0(빈 목록 방어는 _play_current 의 while 이 담당).
    a = _adapter()
    a._voice = SimpleNamespace(
        is_connected=lambda: True, stop=lambda: pytest.fail("건드리면 안 됨")
    )
    a._music_entries = [{"id": "only"}]
    assert asyncio.run(a._dequeue_coro("only")) == 0
    assert [e["id"] for e in a._music_entries] == ["only"]
    # 큐에 없는 id·미재생·정지 중도 no-op.
    assert asyncio.run(a._dequeue_coro("없는id")) == 0
    a._music_stopping = True
    assert asyncio.run(a._dequeue_coro("only")) == 0
    a._voice = None
    assert asyncio.run(a._dequeue_coro("only")) == 0


def test_music_play_one_coro_moves_queued_song_next(monkeypatch):
    # 'ㅁ재생 <제목>': 큐에서 그 곡을 꺼내 현재 곡 바로 뒤로 옮기고 stop() → _advance 가 그 곡으로.
    a, _played, sent = _playing(
        monkeypatch,
        [
            {"id": "v0", "title": "곡0"},
            {"id": "v1", "title": "현재곡"},
            {"id": "v2", "title": "밤편지"},
        ],
        index=1,
    )
    a._caller_voice_channel = lambda _uid: object()  # type: ignore[method-assign]
    assert asyncio.run(a._music_play_one_coro(1, 2, "밤편지")) == ""  # 성공 회신 없음
    assert sent == []  # 이 시점엔 아무것도 안 나갔다 — 알림은 _advance 뒤 1건뿐
    assert a._music_stopping is False  # stopping 을 건드리면 advance 가 죽는다
    assert len(a._music_entries) == 3  # 이동일 뿐 중복 삽입 아님
    asyncio.run(a._advance())  # 실제 _advance 로 확인
    assert sent == [(500, "💿 현재 재생 곡\n`밤편지`")]


def test_music_play_one_coro_restarts_current_song(monkeypatch):
    # 재생 중인 곡 **자신**을 지정하면 그 곡을 처음부터 다시 튼다(인덱스가 어긋나지 않는다).
    a, _played, sent = _playing(
        monkeypatch,
        [{"id": "a", "title": "A"}, {"id": "cur", "title": "현재곡"}, {"id": "b", "title": "B"}],
        index=1,
    )
    a._caller_voice_channel = lambda _uid: object()  # type: ignore[method-assign]
    assert asyncio.run(a._music_play_one_coro(1, 2, "현재곡")) == ""
    assert [e["id"] for e in a._music_entries] == ["a", "cur", "b"]  # 구성 불변
    asyncio.run(a._advance())
    assert sent == [(500, "💿 현재 재생 곡\n`현재곡`")]


def test_music_play_one_coro_search_fallback_and_failure():
    # 큐에 없으면 유튜브 검색 폴백(재생목록엔 추가 안 함 — 임시 편입). 검색도 실패면 안내 문자열.
    a = _adapter()
    a._voice = SimpleNamespace(is_connected=lambda: True, stop=lambda: None)
    a._caller_voice_channel = lambda _uid: object()  # type: ignore[method-assign]
    a._music_entries = [{"id": "v0", "title": "곡0"}]
    a._music_index = 0
    a.search_video = lambda _q, _i=0: ("newvid", "새로운곡")  # type: ignore[method-assign]
    assert asyncio.run(a._music_play_one_coro(1, 2, "새로운곡")) == ""
    assert [e["id"] for e in a._music_entries] == ["v0", "newvid"]
    a.search_video = lambda _q, _i=0: None  # type: ignore[method-assign]
    assert asyncio.run(a._music_play_one_coro(1, 2, "없는곡")) == "재생 실패(제목 검색 실패)"
    assert len(a._music_entries) == 2  # 실패는 큐를 건드리지 않는다


def test_music_play_one_failure_does_not_echo_raw_markdown():
    """L-1: 실패 회신이 사용자 입력을 되돌리면 안 된다.

    플레이리스트 채널은 비인가 서버 멤버가 우회 허용 대상이라, 마크다운 링크를 치면 **봇 명의로**
    피싱 링크가 게시된다(멘션은 막았지만 링크는 아니다). 회신은 입력을 아예 싣지 않는 고정 문구다.
    """
    a = _adapter()
    a._voice = SimpleNamespace(is_connected=lambda: True, stop=lambda: None)
    a._caller_voice_channel = lambda _uid: object()  # type: ignore[method-assign]
    a._music_entries = [{"id": "v0", "title": "곡0"}]
    a.search_video = lambda _q, _i=0: None  # type: ignore[method-assign]
    reply = asyncio.run(a._music_play_one_coro(1, 2, "[지금 확인](https://피싱주소)"))
    assert reply == "재생 실패(제목 검색 실패)"
    # 백틱을 섞어 코드스팬을 닫고 빠져나오려는 시도도, 회신 도배용 긴 입력도 한 글자도 안 실린다.
    assert asyncio.run(a._music_play_one_coro(1, 2, "`](https://x) @everyone")) == reply
    assert asyncio.run(a._music_play_one_coro(1, 2, "가" * 200)) == reply


def _startable(monkeypatch, flat, search=None):
    """**미재생 상태**에서 `_music_play_coro` 를 실제로 태울 수 있는 어댑터(음성 연결만 가짜).

    'ㅁ재생 <제목>' 을 안 틀어져 있을 때 쓰는 경로 = 요구사항의 절반인데 실행 0줄이었다(B2).
    """
    a, played, sent = _playing(monkeypatch, [])
    voice = a._voice
    a._voice = None  # 아직 미연결
    a._music_playlist_url = "https://pl"
    a._extract_flat = lambda: [dict(e) for e in flat]  # type: ignore[method-assign]
    a.search_video = lambda _q, _i=0: search  # type: ignore[method-assign]

    async def connect(**_kw):
        a._voice = voice
        return voice

    a._caller_voice_channel = lambda _uid: SimpleNamespace(connect=connect)  # type: ignore[method-assign]
    return a, played, sent


def _now_playing(a):
    return a._music_entries[a._music_index]


def test_music_play_coro_query_hits_queue(monkeypatch):
    # ㅁ재생 <제목>: 재생목록에 있으면 셔플 첫 곡 대신 **그 곡부터** 시작한다.
    flat = [
        {"id": "a", "title": "곡A"},
        {"id": "b", "title": "밤편지"},
        {"id": "c", "title": "곡C"},
    ]
    a, _played, sent = _startable(monkeypatch, flat)
    assert asyncio.run(a._music_play_coro(500, 7, "밤편지")) == ""
    assert _now_playing(a)["id"] == "b"
    assert sent == [(500, "💿 현재 재생 곡\n`밤편지`")]


def test_music_play_coro_query_search_fallback(monkeypatch):
    # 재생목록에 없으면 유튜브 검색으로 끌어와 맨 앞에 끼우고 그 곡부터.
    flat = [{"id": "a", "title": "곡A"}, {"id": "b", "title": "곡B"}]
    a, _played, sent = _startable(monkeypatch, flat, search=("newvid", "새로운곡"))
    assert asyncio.run(a._music_play_coro(500, 7, "새로운곡")) == ""
    assert _now_playing(a) == {"id": "newvid", "title": "새로운곡", "queued": True}
    assert sent == [(500, "💿 현재 재생 곡\n`새로운곡`")]


def test_music_play_coro_query_search_failure_starts_shuffled(monkeypatch):
    # 검색까지 실패하면 재생 자체를 막지 않는다 — 셔플 첫 곡(index 0)으로 시작.
    flat = [{"id": "a", "title": "곡A"}, {"id": "b", "title": "곡B"}]
    a, _played, sent = _startable(monkeypatch, flat, search=None)
    assert asyncio.run(a._music_play_coro(500, 7, "없는곡")) == ""
    assert a._music_index == 0 and len(a._music_entries) == 2  # 끼워넣지 않았다
    assert sent[0][1].startswith("💿 현재 재생 곡\n`곡")


def test_music_play_coro_sends_reply_before_now_playing(monkeypatch):
    """🔴 'ㅁ노래' 는 **회신 → 곡 알림** 순서다(2026-08-18 운영자 지적).

    코어가 반환값을 보내던 종전 구조에서는 _play_current 의 '💿' 알림이 먼저 나가 순서가
    뒤집혔다. 그래서 어댑터가 재생 **전에** 직접 보내고 반환은 ""(= 코어 미발송)이다.
    """
    flat = [{"id": "a", "title": "곡A"}, {"id": "b", "title": "곡B"}]
    a, _played, sent = _startable(monkeypatch, flat)
    assert asyncio.run(a._music_play_coro(500, 7)) == ""  # 코어가 또 보내지 않게
    assert len(sent) == 2
    assert sent[0] == (500, "▶️ Play - 2곡")  # ① 회신이 먼저
    assert sent[1][1].startswith(f"{discord_adapter.MUSIC_NOW_HEADER}\n")  # ② 곡 알림이 뒤


def test_music_play_coro_failures_still_reply(monkeypatch):
    """🔴 성공만 "" 다 — 실패는 회신이 남아야 한다(없애면 아무 반응이 없어 죽은 것처럼 보인다)."""
    a, _played, sent = _startable(monkeypatch, [{"id": "a", "title": "곡A"}])
    a._music_playlist_url = ""
    assert asyncio.run(a._music_play_coro(500, 7)) == "⚠️재생목록 설정필요"
    a._music_playlist_url = "https://pl"
    a._caller_voice_channel = lambda _uid: None  # type: ignore[method-assign]
    assert asyncio.run(a._music_play_coro(500, 7)) == "🔊 먼저 음성채널에 들어가 주세요"
    assert sent == []  # 실패 경로는 어댑터가 직접 보내지 않는다(코어가 회신을 보낸다)
    # 루프 미준비(_run → None)면 공개 표면이 안내로 폴백한다 — ""(미발송)로 새면 안 된다.
    assert _adapter().play_music(1, 2) == "⚠️오류 발생"


def test_music_play_coro_query_has_no_reply(monkeypatch):
    # 'ㅁ재생 <제목>'(미재생 상태) — '🎵 N곡 재생목록' 회신을 없앴다. 💿 알림 1건만 나간다.
    flat = [{"id": "a", "title": "곡A"}, {"id": "b", "title": "밤편지"}]
    a, _played, sent = _startable(monkeypatch, flat)
    assert asyncio.run(a._music_play_coro(500, 7, "밤편지")) == ""
    assert sent == [(500, "💿 현재 재생 곡\n`밤편지`")]


def test_music_play_one_coro_requires_caller_in_voice_channel():
    # M-2: 음성채널에 안 들어온 user 는 재생을 못 끊는다 — 검색(yt-dlp)까지 가면 안 된다.
    a = _adapter()
    a._voice = SimpleNamespace(is_connected=lambda: True, stop=lambda: pytest.fail("stop 금지"))
    a._music_entries = [{"id": "v0", "title": "곡0"}]
    a._caller_voice_channel = lambda _uid: None  # type: ignore[method-assign]
    a.search_video = lambda _q, _i=0: pytest.fail("검색하면 안 된다")  # type: ignore[method-assign]
    assert asyncio.run(a._music_play_one_coro(1, 2, "없는곡")) == "🔊 먼저 음성채널에 들어가 주세요"
    assert len(a._music_entries) == 1  # 큐 무변형


def test_music_play_one_coro_delegates_while_stopping():
    # 정지 진행 중이면 큐를 만져봐야 _advance 가 즉시 return 해 아무것도 안 나온다 →
    # 통상 재생으로 위임(_music_play_coro 가 _music_stopping 을 리셋하며 정상 시작).
    a = _adapter()
    a._caller_voice_channel = lambda _uid: object()  # type: ignore[method-assign]
    a._voice = SimpleNamespace(is_connected=lambda: True, stop=lambda: pytest.fail("stop 금지"))
    a._music_entries = [{"id": "v0", "title": "곡0"}]
    a._music_stopping = True
    calls = []

    async def fake_play(cid, uid, q=""):
        calls.append((cid, uid, q))
        return ""

    a._music_play_coro = fake_play  # type: ignore[method-assign]
    assert asyncio.run(a._music_play_one_coro(1, 2, "곡0")) == ""
    assert calls == [(1, 2, "곡0")]
    assert len(a._music_entries) == 1  # 큐 무변형


def test_client_blocks_all_mentions():
    # 🔴 M-1: 유튜브 제목이 곡마다 채널로 나가므로 멘션을 뿌리(클라이언트)에서 막는다.
    # 이 줄이 없으면 디스코드가 본문의 @everyone·유저 멘션을 파싱해 봇 명의로 핑을 쏜다.
    am = _adapter()._client.allowed_mentions
    assert (am.everyone, am.users, am.roles, am.replied_user) == (False, False, False, False)


def test_send_kwargs_never_reenable_mentions():
    # SNS 판정완료 카드처럼 외부 유래 문자열(@everyone·<@id>·<@&role>)이 실린 본문도 전송 kwargs 가
    # allowed_mentions 를 덮지 않아 클라이언트 전역 none 이 그대로 적용된다.
    for payload in ("@everyone <@123> <@&456> @here", discord.Embed(description="@everyone")):
        assert "allowed_mentions" not in discord_adapter._send_kwargs(payload, None)


def test_find_title_folds_spaces_and_case():
    # 큐 매칭 = 공백접기+casefold(adapter.fold_title) 부분 포함. ⚠️ 정규화만 remove_video 와
    # 공유하고 선택 규칙은 다르다(삭제=정확일치 우선, 재생=부분 첫 곡 — 계약 §1 동결분).
    entries = [{"id": "v1", "title": "밤편지"}, {"id": "v2", "title": "IU - Good Day"}]
    assert discord_adapter._find_title(entries, "good day") == 1
    assert discord_adapter._find_title(entries, "  밤 편지 ") == 0
    assert discord_adapter._find_title(entries, "없는곡") is None
    assert discord_adapter._find_title([{"id": "vid_only"}], "vid_only") == 0  # title 없으면 id


def test_enqueue_coro_noop_when_not_playing():
    a = _adapter()
    a._voice = None  # 재생 중 아님
    a._music_entries = []
    assert asyncio.run(a._enqueue_coro("newvid", "새곡")) == 0
    assert a._music_entries == []
    # voice 있어도 큐 비었으면(재생 준비 전) no-op.
    a._voice = SimpleNamespace(is_connected=lambda: True)
    assert asyncio.run(a._enqueue_coro("newvid", "새곡")) == 0


def test_enqueue_coro_noop_when_stopping():
    a = _adapter()
    a._voice = SimpleNamespace(is_connected=lambda: True)
    a._music_entries = [{"id": "cur0"}]
    a._music_stopping = True  # 정지 진행 중 — 편입 안 함
    assert asyncio.run(a._enqueue_coro("newvid", "새곡")) == 0
    assert len(a._music_entries) == 1


def test_message_event_channel_map_project_and_role():
    a = _adapter()
    a._channel_map = {100: ("project", "etf_info"), 200: ("role", "데이터분석")}
    ev_p = a._message_event(_msg(777, "hi", channel_id=100, channel_name="딴이름"))
    assert ev_p.project == "etf_info" and ev_p.channel_role is None  # 채널ID 매핑 우선
    ev_r = a._message_event(_msg(777, "hi", channel_id=200))
    assert ev_r.channel_role == "데이터분석" and ev_r.project is None


def test_message_event_unmapped_channel_has_no_project_or_role():
    # 채널명을 프로젝트 후보로 쓰던 폴백은 프로젝트 원격 작업과 함께 삭제됐다.
    a = _adapter()  # channel_map 비어있음
    ev = a._message_event(_msg(777, "hi", channel_id=100, channel_name="trading_info"))
    assert ev.project is None and ev.channel_role is None


def test_message_event_image_attachment_is_still_a_text_event():
    # 사진 + 지시(photo 이벤트)는 삭제됐다 — 첨부가 있어도 본문만 text 로 정규화한다.
    a = _adapter()
    m = _msg(777, "캡션", channel_id=100)
    m.attachments = [SimpleNamespace(filename="a.png", url="https://cdn.discordapp.com/a.png")]
    ev = a._message_event(m)
    assert ev.kind == "text" and ev.text == "캡션"


class _FakeOverwrite:
    """`discord.PermissionOverwrite` 최소 모사 — 비트별 True/False/None(상속).

    ⚠️ **여러 비트를 담는 것이 핵심이다** — 종전 페이크는 `send_messages` 하나만 저장해
    "다른 권한이 존재하지 않는 세계"를 만들었고, 그래서 `set_permissions` 가 오버라이트를
    **통째로 치환**한다는 사실을 1,114개 테스트 중 어느 것도 볼 수 없었다.
    비트 목록을 고정하지 않는다 — 프로덕션이 새 권한을 만지면 그대로 따라간다.
    """

    def __init__(self, **bits):
        self.send_messages = self.view_channel = None  # 자주 읽는 것은 기본값을 둔다
        self.__dict__.update(bits)

    def as_dict(self):
        return {k: v for k, v in vars(self).items() if v is not None}


class _FakePermissions:
    def __init__(self, view_channel):
        self.view_channel = view_channel


def _as_bits(value):
    """페이크 입력 축약형 허용 — `True/False/None` 은 `send_messages` 로 읽는다."""
    return dict(value) if isinstance(value, dict) else {"send_messages": value}


class _FakeChannel:
    def __init__(self, cid, name, position=0, reject_rename=False, overwrites=None):
        self.id = cid
        self.name = name
        self.position = position
        self.deleted = False
        self.reject_rename = reject_rename  # 디스코드 이름 거부 시나리오
        self.renames = []  # edit(name=…) 시도 기록
        # target → {권한명: True/False}. 축약형(bool)도 받아 기존 테스트 호출부를 그대로 둔다.
        self.overwrites = {t: _as_bits(v) for t, v in (overwrites or {}).items()}
        self.perm_calls = []  # (target, 적용된 비트) — 멱등·보존 확인용

    def bit(self, target, name="send_messages"):
        """저장된 권한 비트 하나(없으면 None = 상속)."""
        return self.overwrites.get(target, {}).get(name)

    def overwrites_for(self, target):
        return _FakeOverwrite(
            **self.overwrites.get(target, {})
        )  # 매번 **새 객체**(discord.py 동형)

    def permissions_for(self, target):
        """유효 권한 — **채널 오버라이트만** 접는다(멤버 → @everyone → 기본 허용).

        ⚠️ **한계: 길드 역할 권한·채널 역할 오버라이트·Administrator 를 모델링하지 않는다.**
        진짜 `discord.abc.GuildChannel.permissions_for` 는 길드 기본권한(@everyone 역할 +
        멤버의 역할들) → 채널 @everyone → 채널 역할 → 채널 멤버 순으로 접고 Administrator 는
        단축한다. **그 네 경로가 걸린 시나리오는 이 층에서 끝내 검증되지 않는다** — 프로덕션이
        `permissions_for` 에 위임하는 근거는 이 페이크가 아니라 그 소스와 실측 대조다.
        "테스트가 다 통과하니 안전하다"고 읽지 마라.
        """
        for key in (target, "@everyone"):
            value = self.overwrites.get(key, {}).get("view_channel")
            if value is not None:
                return _FakePermissions(value)
        return _FakePermissions(True)  # 오버라이트가 없으면 기본 허용

    async def set_permissions(self, target, *, overwrite=None, **kwargs):
        # ⚠️ 실제 discord.py 2.7.1 과 동일하게 **전체 치환**이다 — kwargs 형이든 overwrite 형이든
        # 그 대상의 오버라이트를 통째로 갈아끼운다(부분 갱신이 아니다).
        bits = overwrite.as_dict() if overwrite is not None else dict(kwargs)
        self.perm_calls.append((target, bits))
        self.overwrites[target] = bits

    async def edit(self, *, name=None, position=None, **_kwargs):
        if name is not None:
            self.renames.append(name)
            if self.reject_rename:
                raise discord.DiscordException("name rejected")
            self.name = name
        if position is not None:
            self.position = position

    async def delete(self):
        self.deleted = True


class _FakeCategory:
    def __init__(self, name, position=0, channels=None):
        self.name = name
        self.position = position
        self.channels = list(channels or [])  # 자식(빈 카테고리 판정)
        self.deleted = False
        self.renames = []  # edit(name=…) 기록

    async def edit(self, *, name=None, position=None):
        if name is not None:
            self.renames.append(name)
            self.name = name
        if position is not None:
            self.position = position

    async def delete(self):
        self.deleted = True


class _FakeGuild:
    def __init__(self, text_channels=None, voice_channels=None, categories=None):
        self.text_channels = list(text_channels or [])
        self.voice_channels = list(voice_channels or [])
        self.categories = list(categories or [])
        self.created = []  # (name, topic) — 텍스트 생성
        self.voice_created = []  # 음성 생성 name
        self._next = 1000
        self.default_role = "@everyone"  # 권한 오버라이트 대상(읽기전용 채널)
        self.me = "bot"  # 봇 자신 — 함께 잠기면 다이제스트가 못 나간다

    @property
    def channels(self):  # by_id 용(텍스트+음성 — 카테고리 id 는 매핑에 불필요)
        return [*self.text_channels, *self.voice_channels]

    async def create_category(self, name):
        await asyncio.sleep(0)  # suspension point — F1 동시 on_ready 재진입 재현용
        cat = _FakeCategory(name, position=len(self.categories))
        self.categories.append(cat)
        return cat

    async def create_text_channel(self, name, **kwargs):
        self._next += 1
        # 디스코드 저장 모사: ASCII 소문자화(붙여쓰기명은 공백·하이픈 없음 — 언더스코어만 유지)
        ch = _FakeChannel(self._next, name.lower())
        self.text_channels.append(ch)
        self.created.append((name, kwargs.get("topic")))
        return ch

    async def create_voice_channel(self, name, **_kwargs):
        self._next += 1
        ch = _FakeChannel(self._next, name)  # 음성은 공백·대소문자 허용(변형 없음)
        self.voice_channels.append(ch)
        self.voice_created.append(name)
        return ch


def test_ensure_channels_creates_categories_channels_and_persists(tmp_path):
    cm = tmp_path / "channel_map.json"
    a = DiscordAdapter("tok", [], _ALLOWED, channel_map_file=cm)
    guild = _FakeGuild()
    a._client = SimpleNamespace(guilds=[guild])  # type: ignore[assignment]
    asyncio.run(a._ensure_channels())
    tags = set(a._channel_map.values())
    assert ("role", "봇상태") in tags
    assert ("role", "알림") not in tags  # 검증 카드 기능 삭제(2026-10-08) — 더는 만들지 않는다
    assert a.role_channel("봇상태") is not None and a.role_channel("알림") is None
    assert cm.exists()  # 영속
    assert discord_adapter.load_channel_map(cm) == a._channel_map


def test_ensure_channels_creates_no_project_channels(tmp_path):
    """새 `_Project/*` 폴더가 생겨도 채널을 만들지 않는다(동기화 로직 삭제).

    어댑터는 프로젝트 목록을 받는 입구(`setup_channels`)가 아예 없고, 만드는 채널은 역할 채널
    (미국주식·SNS정보·봇상태)과 PlayList 음성뿐이다. 레포의 `_Project` 폴더가 몇 개든 같다.
    """
    projects = tmp_path / "_Project"
    for name in ("etf_info", "trading_info", "brand_new_project"):
        (projects / name).mkdir(parents=True)
    a = DiscordAdapter("tok", [], _ALLOWED)
    assert not hasattr(a, "setup_channels") and not hasattr(a, "project_channel")
    guild = _FakeGuild()
    a._client = SimpleNamespace(guilds=[guild])  # type: ignore[assignment]
    asyncio.run(a._ensure_channels())
    assert {n for n, _ in guild.created} == {"마이크론", "sns정보", "알림"}
    assert guild.voice_created == ["PlayList"]
    assert {kind for kind, _tag in a._channel_map.values()} == {"role"}
    assert not any("프로젝트" in c.name for c in guild.categories)


def test_ensure_channels_keeps_old_project_entries_untouched(tmp_path):
    """옛 프로젝트 채널은 만들지도·고치지도·지우지도 않지만 맵에는 그대로 남긴다.

    맵에서 빠지면 그 채널이 미매핑(역할 없음)이 되어 코어가 «프로젝트 채널 → 무시» 로 알아보지
    못한다. 서버의 채널 삭제는 사람 몫이고, 그때까지 맵 항목은 읽기만 한다.
    """
    cm = tmp_path / "channel_map.json"
    old = _FakeChannel(500, "주식모니터링", position=3)
    cm.write_text('{"500": ["project", "trading_info"]}', encoding="utf-8")
    a = DiscordAdapter("tok", [], _ALLOWED, channel_map_file=cm)
    guild = _guild_for(a, text_channels=[old])
    asyncio.run(a._ensure_channels())
    assert a._channel_map[500] == ("project", "trading_info")
    assert old.renames == [] and old.deleted is False and old.position == 3
    assert "trading_info" not in {n for n, _ in guild.created}
    assert discord_adapter.load_channel_map(cm)[500] == ("project", "trading_info")
    ev = a._message_event(_msg(777, "고쳐줘", channel_id=500))
    assert ev.project == "trading_info" and ev.channel_role is None


def test_loading_old_channel_map_with_project_entries_does_not_crash(tmp_path):
    cm = tmp_path / "channel_map.json"
    cm.write_text('{"1": ["project", "etf_info"], "2": ["role", "봇상태"]}', encoding="utf-8")
    a = DiscordAdapter("tok", [], _ALLOWED, channel_map_file=cm)
    assert a.role_channel("봇상태") == 2 and a._channel_map[1] == ("project", "etf_info")


def test_concurrent_on_ready_no_duplicate():
    # F1: 첫 셋업 중 reconnect 로 on_ready 2회 겹쳐도 _setup_lock 이 직렬화 → 중복 생성 없음.
    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _guild_for(a)

    async def two_on_ready():
        await asyncio.gather(a._ensure_channels(), a._ensure_channels())

    asyncio.run(two_on_ready())
    # 카테고리 3개(스케쥴러·시스템·PlayList) — 프로젝트 카테고리는 만들지 않는다.
    assert len(guild.categories) == 3
    assert [n for n, _ in guild.created].count("마이크론") == 1


def test_ensure_channels_no_guild_skips(tmp_path):
    a = DiscordAdapter("tok", [], _ALLOWED, channel_map_file=tmp_path / "cm.json")
    a._client = SimpleNamespace(guilds=[])  # type: ignore[assignment]
    asyncio.run(a._ensure_channels())  # 길드 없음 → 스킵(예외 없이)
    assert a._channel_map == {}


# ---------------------------------------------------------------------------
# on_ready 서버 닉 고정(_ensure_nickname) — 멱등·권한실패 방어
# ---------------------------------------------------------------------------


class _FakeMember:
    def __init__(self, nick=None, raise_exc=None):
        self.nick = nick
        self._raise = raise_exc
        self.edits = []  # 넘겨받은 nick 기록

    async def edit(self, *, nick):
        if self._raise is not None:
            raise self._raise
        self.edits.append(nick)
        self.nick = nick


def _guild_with_me(me):
    return SimpleNamespace(id=1, me=me)


def test_ensure_nickname_sets_when_different():
    a = _adapter()
    me = _FakeMember(nick="옛닉")
    a._client = SimpleNamespace(guilds=[_guild_with_me(me)])  # type: ignore[assignment]
    asyncio.run(a._ensure_nickname())
    assert me.edits == [discord_adapter._BOT_NICKNAME]  # 다르면 edit


def test_ensure_nickname_skips_when_already_set():
    a = _adapter()
    me = _FakeMember(nick=discord_adapter._BOT_NICKNAME)
    a._client = SimpleNamespace(guilds=[_guild_with_me(me)])  # type: ignore[assignment]
    asyncio.run(a._ensure_nickname())
    assert me.edits == []  # 멱등: 같으면 edit 안 함(레이트리밋 회피)


def test_ensure_nickname_swallows_permission_error():
    a = _adapter()
    me = _FakeMember(nick="옛닉", raise_exc=discord.DiscordException("no perm"))
    a._client = SimpleNamespace(guilds=[_guild_with_me(me)])  # type: ignore[assignment]
    asyncio.run(a._ensure_nickname())  # Forbidden 등 → 삼키고 계속(예외 전파 없음)


def test_ensure_channels_channel_create_failure_skips_but_maps_rest():
    # "실패는 로그+계속"(§4.4) 회귀 잠금: 한 채널 생성이 디스코드 예외로 실패해도 전체 setup 이
    # 중단되지 않고 나머지 채널은 정상 매핑된다(권한 오류 1건이 전체 라우팅을 지우지 않게).
    class _RejectingGuild(_FakeGuild):
        async def create_text_channel(self, name, **kwargs):
            if name == "마이크론":  # 특정 채널만 생성 거부(권한 없음 모사)
                raise discord.DiscordException("forbidden")
            return await super().create_text_channel(name, **kwargs)

    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _RejectingGuild()
    a._client = SimpleNamespace(guilds=[guild])  # type: ignore[assignment]
    asyncio.run(a._ensure_channels())  # 예외로 안 죽음
    tags = set(a._channel_map.values())
    assert ("role", "미국주식") not in tags  # 실패한 채널은 미매핑(스킵)
    # 다른 특수채널은 정상 매핑(부분 실패가 전체를 무너뜨리지 않음)
    assert {("role", "봇상태"), ("role", "SNS정보")} <= tags


# ---------------------------------------------------------------------------
# ① 후속: 프로젝트 채널명=한글 라벨(리네임)·음성 PlayList·카테고리 순서·기본 #일반 삭제·멱등
# ---------------------------------------------------------------------------


def _guild_for(a, **kw):
    guild = _FakeGuild(**kw)
    a._client = SimpleNamespace(guilds=[guild])  # type: ignore[assignment]
    return guild


def test_special_channel_names_are_joined():
    # 특수 채널명 붙여쓰기(하이픈 없음): 표시명 알림(tag 봇상태) 등. (빈이름 폐기 — 정상명).
    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    created = {n for n, _ in guild.created}
    assert "알림" in created and "봇상태" not in created  # 표시명만 알림 — tag 는 봇상태
    assert not any("-" in n for n in created)  # 하이픈 없음


def test_rename_rejected_keeps_mapping():
    # 디스코드가 리네임 거부(400 등) → 기존명 보존하되 channel_map 매핑은 유지(라우팅 안 깨짐).
    a = DiscordAdapter("tok", [], _ALLOWED)
    simple = _FakeChannel(700, "구이름", reject_rename=True)  # 목표명과 달라 리네임 시도됨
    a._channel_map = {700: ("role", "봇상태")}
    _guild_for(a, text_channels=[simple])
    asyncio.run(a._ensure_channels())
    assert simple.renames == ["알림"] and simple.name == "구이름"  # 시도했으나 거부→기존명 보존
    assert a._channel_map[700] == ("role", "봇상태")  # 매핑 유지(라우팅 OK)


def test_voice_playlist_renames_default_general():
    a = DiscordAdapter("tok", [], _ALLOWED)
    default_voice = _FakeChannel(900, "일반")
    guild = _guild_for(a, voice_channels=[default_voice])
    asyncio.run(a._ensure_channels())
    assert default_voice.renames == ["PlayList"]  # 기본음성 → PlayList 리네임(삭제+생성 아님)
    assert guild.voice_created == []  # 새 음성 생성 안 함
    assert a._channel_map[900] == ("role", "playlist")


def test_voice_playlist_created_when_no_default():
    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    assert guild.voice_created == ["PlayList"]
    assert ("role", "playlist") in set(a._channel_map.values())


def test_voice_playlist_idempotent_when_named():
    # 이미 'PlayList' 이면 재기동해도 리네임 안 함(정확 일치 skip, 실패 재시도 없음).
    a = DiscordAdapter("tok", [], _ALLOWED)
    existing = _FakeChannel(901, "PlayList")
    a._channel_map = {901: ("role", "playlist")}
    guild = _guild_for(a, voice_channels=[existing])
    asyncio.run(a._ensure_channels())
    assert guild.voice_created == [] and existing.renames == []


def test_categories_ordered():
    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    order = {discord_adapter._cat_core(c.name): c.position for c in guild.categories}
    assert order["스케쥴러"] == 0  # 🗓️ 스케쥴러 (시스템 위)
    assert order["시스템"] == 1
    assert order["playlist"] == 2  # 🎵 PlayList (시스템 아래)


def test_scheduler_category_in_order_before_system():
    # 🗓️ 스케쥴러 = _CAT_ORDER index 0(시스템 위). 📁 프로젝트 카테고리는 더 이상 만들지 않는다.
    assert discord_adapter._CAT_ORDER == [
        discord_adapter._CAT_SCHED,
        discord_adapter._CAT_SYSTEM,
        discord_adapter._CAT_VOICE,
    ]
    assert discord_adapter._CAT_ALIASES[discord_adapter._CAT_SCHED] == ["스케쥴러"]
    assert not hasattr(discord_adapter, "_CAT_PROJECT")


def test_scheduler_special_role_channels():
    """**표시명과 tag 가 일부러 다르다** — 표시명은 `마이크론`, tag 는 `미국주식` 그대로.

    tag 를 표시명에 맞추면 채널 탐색 1차(`channel_map` 의 `(kind, tag)`)·2차(이름 canon)가
    모두 빗나가 **새 채널이 생긴다** — 옛 채널의 히스토리도 손으로 건 읽기전용 권한도 안 따라온다.
    표시명만 바꾸면 `_rename_if_needed` 가 기존 채널을 제자리에서 rename 한다.
    """
    assert discord_adapter._SPECIAL[discord_adapter._CAT_SCHED] == [
        ("마이크론", "role", "미국주식"),
        # `개발자료` 는 2026-08-15 제거 — 이 단언이 **되살아나는 것을 막는 자물쇠**다
        # (목록에 다시 들어가면 재기동이 채널을 자동생성한다).
        ("SNS정보", "role", "SNS정보"),  # 사람이 링크를 공유 — 읽기전용 아님(아래 단언)
    ]
    assert "SNS정보" not in discord_adapter._READONLY_TAGS
    # ⚠️ 이 순서는 **최초 생성 순서**만 정한다(새 채널은 카테고리 맨 아래에 붙는다).
    # **이미 있는 채널의 position 은 코드가 안 건드린다** — 목록을 재배열해도 라이브는 안 움직인다.
    # `edit(position=…)` 은 `_reorder_projects`(프로젝트)·`_order_categories`(카테고리)뿐이다.
    # 표시 순서를 바꾸려면 디스코드에서 직접 옮긴다.
    # (종전 주석은 *"바꾸면 채널이 재배치된다"* 였으나 **거짓이었다** — 2026-08-15 점검에서
    #  두 게이트가 독립 실측: 항목 제거 전후 4채널 position 불변. 거짓 주석은 없느니만 못하다.)
    # notify.json 의 `channel` 은 **표시명이 아니라 tag** 를 쓴다:
    # us-digest → "미국주식"(표시명 `#마이크론` 아님).
    # 읽기전용·notify.json 라우팅도 tag 기준이라 함께 무변경이어야 한다.
    assert "미국주식" in discord_adapter._READONLY_TAGS


def test_scheduler_channels_created_and_mapped():
    # 새 카테고리 🗓️ 스케쥴러 아래에 마이크론 role 채널 생성 + channel_map 매핑.
    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    tags = set(a._channel_map.values())
    assert ("role", "미국주식") in tags
    created = {n for n, _ in guild.created}
    assert "마이크론" in created
    # 라우팅 키는 **tag** 다 — 표시명이 바뀌어도 `notify.json` 의 `channel: "미국주식"` 이 산다.
    assert a.role_channel("미국주식") is not None
    assert any(discord_adapter._cat_core(c.name) == "스케쥴러" for c in guild.categories)


def test_existing_us_channel_is_renamed_not_recreated():
    """표시명 변경은 **기존 채널 rename** 이어야 한다 — 새로 만들면 히스토리·권한이 사라진다."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    old = _FakeChannel(777, "미국주식", overwrites={"@everyone": False, "bot": True})
    a._channel_map = {777: ("role", "미국주식")}
    guild = _guild_for(a, text_channels=[old])
    asyncio.run(a._ensure_channels())
    assert old.name == "마이크론"  # 제자리 rename
    assert "마이크론" not in {n for n, _ in guild.created}  # 새 채널을 만들지 않았다
    assert a._channel_map[777] == ("role", "미국주식")  # tag 는 그대로 → 라우팅 유지


def test_us_channel_is_readonly_for_people_but_writable_by_bot():
    # #마이크론 은 읽기 전용(사용자: "매일 보는 용도로만"). **봇까지 잠그면 카드가 못 나간다.**
    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    ch = next(c for c in guild.text_channels if c.name == "마이크론")
    assert ch.bit("@everyone") is False  # 사람은 못 쓴다
    assert ch.bit("bot") is True  # 봇은 쓴다
    others = [c for c in guild.text_channels if c.name in ("알림",)]
    assert others and all(c.perm_calls == [] for c in others)  # 다른 채널은 안 건드린다


def test_us_channel_readonly_is_idempotent():
    # 이미 적용돼 있으면 아무것도 하지 않는다(재기동마다 감사 로그를 남기지 않게).
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식", overwrites={"@everyone": False, "bot": True})
    a._channel_map = {777: ("role", "미국주식")}
    _guild_for(a, text_channels=[ch])
    asyncio.run(a._ensure_channels())
    assert ch.perm_calls == []


@pytest.mark.parametrize(
    ("overwrites", "want_targets"),
    [
        # ① 이미 읽기 전용(사람이 채널 설정에서 손으로 해둔 상태 = 현재 실서버) → 호출 0
        ({"@everyone": False, "bot": True}, []),
        # ② 미설정 → 봇 허용 **먼저**, 그다음 @everyone 거부
        ({}, ["bot", "@everyone"]),
        # ③ @everyone 만 거부 · 봇 누락 = **가장 위험**(봇이 잠긴다) → "완료" 로 읽지 말고 봇을 푼다
        ({"@everyone": False}, ["bot"]),
        # ④ 봇이 상속(None)뿐 → 명시 허용이 아니므로 @everyone 거부에 걸린다 → 봇을 푼다
        ({"@everyone": False, "bot": None}, ["bot"]),
        # ⑤ 봇만 허용 · 사람 미차단(반대쪽 절반) → 이미 된 봇은 다시 안 부르고 @everyone 만
        ({"bot": True}, ["@everyone"]),
    ],
)
def test_ensure_readonly_reads_first_then_fixes_only_whats_missing(overwrites, want_targets):
    """**먼저 읽고 필요할 때만 쓴다.** 부분 상태를 "완료"로 읽으면 봇이 잠긴 채 방치된다."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식", overwrites=overwrites)
    guild = _FakeGuild(text_channels=[ch])
    asyncio.run(a._ensure_readonly(guild, ch))
    assert [t for t, _kw in ch.perm_calls] == want_targets
    assert all(kw == {"send_messages": t == "bot"} for t, kw in ch.perm_calls)


def test_ensure_readonly_keeps_everyone_view_deny_intact():
    """🔴 실서버 상태 재현 — `@everyone` 은 **채널 보기까지 거부**(사용자가 비공개로 만듦)인데
    `send_messages` 만 빠진 경우. `send_messages=` 만 넘기면 오버라이트가 통째로 치환돼
    **비공개 채널이 서버 전체에 공개된다.** 그 회귀를 여기서 못박는다."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(
        777,
        "미국주식",
        overwrites={
            "@everyone": {"view_channel": False},
            "bot": {"send_messages": True, "view_channel": True},
        },
    )
    asyncio.run(a._ensure_readonly(_FakeGuild(text_channels=[ch]), ch))
    assert ch.bit("@everyone", "view_channel") is False  # 비공개 유지 — 지워지면 안 된다
    assert ch.bit("@everyone", "send_messages") is False  # 우리가 건 것
    assert [t for t, _b in ch.perm_calls] == ["@everyone"]  # 봇은 이미 완료라 안 건드린다


def test_ensure_readonly_keeps_the_bots_other_permissions():
    """봇 쪽도 같은 함정 — `채널 보기 ✓`·`앱 명령`이 지워지면 봇이 채널을 못 봐 카드가 안 나간다."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(
        777,
        "미국주식",
        overwrites={
            "@everyone": {"send_messages": False, "view_channel": False},
            "bot": {"view_channel": True, "use_application_commands": True},  # 쓰기만 빠짐
        },
    )
    asyncio.run(a._ensure_readonly(_FakeGuild(text_channels=[ch]), ch))
    assert ch.bit("bot", "send_messages") is True  # 채웠고
    assert ch.bit("bot", "view_channel") is True  # 원래 있던 것은 살아 있다
    assert ch.bit("bot", "use_application_commands") is True
    assert [t for t, _b in ch.perm_calls] == ["bot"]


def test_ensure_readonly_grants_bot_view_when_channel_is_private():
    """@everyone 이 채널 보기까지 막힌 비공개 채널에서 봇 오버라이트가 아예 없으면,
    쓰기만 열어봐야 **채널을 못 봐서** 카드가 안 나간다 → 보기도 함께 켠다(호출은 1회)."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식", overwrites={"@everyone": {"view_channel": False}})
    asyncio.run(a._ensure_readonly(_FakeGuild(text_channels=[ch]), ch))
    assert ch.bit("bot", "send_messages") is True and ch.bit("bot", "view_channel") is True
    assert [t for t, _b in ch.perm_calls] == ["bot", "@everyone"]  # 봇은 한 번만


def test_ensure_readonly_warning_reports_fresh_state_not_stale(caplog):
    """🟡 봇 설정은 성공하고 @everyone 만 실패(5xx·429)했을 때, 경고가 `봇 허용=False` 라고
    말하면 운영자가 **이미 끝난 조치를 다시 하러 간다**. 성공 즉시 값을 갱신해야 한다."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식")
    granted = ch.set_permissions

    async def fail_on_everyone(target, **kwargs):
        if target == "@everyone":
            raise discord.DiscordException("ServerError")
        await granted(target, **kwargs)

    ch.set_permissions = fail_on_everyone  # type: ignore[method-assign]
    with caplog.at_level("WARNING"):
        asyncio.run(a._ensure_readonly(_FakeGuild(text_channels=[ch]), ch))
    assert "봇 허용=True" in caplog.text  # 낡은 False 가 아니라 방금의 성공을 말한다
    assert "@everyone 차단=False" in caplog.text  # 못 건 쪽은 정확히 False


def test_ensure_readonly_Log_nothing_when_already_readonly(caplog):
    """완료 상태면 **로그도 0줄**. 호출 0회만 보면 이 회귀를 못 잡는다.

    안쪽 `if not bot_ok`/`if not blocked` 가 이미 호출을 막으므로, 조기반환을 지워도
    `set_permissions` 는 여전히 0회다 — 대신 끝줄 `log.info` 가 살아나 **재기동마다** 같은
    줄이 쌓인다(이번 수정이 없앤 바로 그 증상). 그래서 호출 수가 아니라 로그로 고정한다.
    """
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식", overwrites={"@everyone": False, "bot": True})
    with caplog.at_level("DEBUG", logger=discord_adapter.log.name):
        asyncio.run(a._ensure_readonly(_FakeGuild(text_channels=[ch]), ch))
    ours = [r.getMessage() for r in caplog.records if r.name == discord_adapter.log.name]
    assert ch.perm_calls == [] and ours == []  # asyncio 자체 DEBUG 는 우리 로그가 아니다


def test_ensure_readonly_warns_with_a_remedy_when_forbidden(caplog):
    """권한이 없으면 **경고를 유지**하되(조용한 실패 금지) 무엇을 하면 되는지 적는다."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식")

    async def deny(*_a, **_k):
        raise discord.DiscordException("Forbidden")

    ch.set_permissions = deny  # type: ignore[method-assign]
    with caplog.at_level("WARNING"):
        asyncio.run(a._ensure_readonly(_FakeGuild(text_channels=[ch]), ch))
    text = caplog.text
    assert "읽기전용 설정 실패" in text
    assert "메시지 보내기" in text and "역할 관리" in text  # 조치 방법이 들어 있다


def test_ensure_readonly_unlocks_the_bot_before_locking_people():
    """순서가 뒤바뀌면 두 번째 호출 실패 시 **봇이 잠긴 채** 남는다 — 봇 허용이 항상 먼저다."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식")
    asyncio.run(a._ensure_readonly(_FakeGuild(text_channels=[ch]), ch))
    assert next(t for t, _kw in ch.perm_calls) == "bot"


def test_ensure_readonly_leaves_the_bot_writable_when_the_second_call_fails():
    """순서 뒤집힘이 실제로 무엇을 망치는지 — **결과**로 고정한다(호출 순서 관찰이 아니라).

    봇 허용은 통과하고 @everyone 거부만 막히는 부분 실패에서, 봇은 **쓸 수 있는 채로** 남아야
    그날 카드가 나간다. 종전 순서(@everyone 먼저)면 여기서 봇 허용이 아예 실행되지 않는다.
    """
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식")
    granted = ch.set_permissions

    async def fail_on_everyone(target, **kwargs):
        if target == "@everyone":
            raise discord.DiscordException("Forbidden")
        await granted(target, **kwargs)

    ch.set_permissions = fail_on_everyone  # type: ignore[method-assign]
    asyncio.run(a._ensure_readonly(_FakeGuild(text_channels=[ch]), ch))
    assert ch.bit("bot") is True  # 봇은 잠기지 않았다 — 카드 경로 생존


@pytest.mark.parametrize("missing", ["default_role", "me"])
def test_ensure_readonly_skips_when_guild_role_info_is_missing(missing, caplog):
    """길드 역할 정보가 없으면 **아무 권한도 건드리지 않고** 경고만 — 추측으로 잠그지 않는다."""
    a = DiscordAdapter("tok", [], _ALLOWED)
    ch = _FakeChannel(777, "미국주식")
    guild = _FakeGuild(text_channels=[ch])
    setattr(guild, missing, None)
    with caplog.at_level("WARNING"):
        asyncio.run(a._ensure_readonly(guild, ch))
    assert ch.perm_calls == []
    assert "길드 역할 정보 없음" in caplog.text


def test_categories_created_with_emoji():
    # #1: 카테고리 헤더에 이모지 표시명으로 생성.
    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    names = {c.name for c in guild.categories}
    assert names == {"🗓️ 스케쥴러", "⚙️ 시스템", "🎵 PlayList"}  # 📁 프로젝트는 만들지 않는다


def test_existing_category_renamed_to_emoji_idempotent():
    # 기존 '스케쥴러'(이모지 없음) → '🗓️ 스케쥴러' 로 rename. 재기동 시 이미 이모지형이면 skip.
    a = DiscordAdapter("tok", [], _ALLOWED)
    plain = _FakeCategory("스케쥴러")
    already = _FakeCategory("⚙️ 시스템")  # 이미 이모지형
    _guild_for(a, categories=[plain, already])
    asyncio.run(a._ensure_channels())
    assert plain.renames == ["🗓️ 스케쥴러"]  # 코어명 매칭 → 이모지 rename
    assert already.renames == []  # 정확 일치 → skip(멱등)


def test_voice_category_renamed_from_old_name():
    # 음성 카테고리 이전 이름 '음성' → '🎵 PlayList' 로 이관(별칭 매칭).
    a = DiscordAdapter("tok", [], _ALLOWED)
    old_voice = _FakeCategory("음성")
    _guild_for(a, categories=[old_voice])
    asyncio.run(a._ensure_channels())
    assert old_voice.renames == ["🎵 PlayList"]


def test_default_general_text_deleted():
    a = DiscordAdapter("tok", [], _ALLOWED)
    general = _FakeChannel(950, "일반")  # 기본 텍스트(맵에 없음 = 봇 생성 아님)
    _guild_for(a, text_channels=[general])
    asyncio.run(a._ensure_channels())
    assert general.deleted is True


def test_default_general_kept_if_bot_channel():
    # 안전장치: 이름이 '일반'이어도 봇이 만든(new_map 등록) 채널이면 삭제 안 함.
    a = DiscordAdapter("tok", [], _ALLOWED)
    a._channel_map = {950: ("project", "weird")}
    botch = _FakeChannel(950, "일반")  # 봇이 라벨 '일반'으로 만든 채널(맵에 있음)
    _guild_for(a, text_channels=[botch])
    asyncio.run(a._ensure_channels())
    assert botch.deleted is False


def test_empty_default_categories_deleted():
    # #5: 비어있는 기본 카테고리(채팅 채널/음성 채널)만 삭제 — 이중 가드.
    a = DiscordAdapter("tok", [], _ALLOWED)
    empty_text = _FakeCategory("채팅 채널", channels=[])
    empty_voice = _FakeCategory("Voice Channels", channels=[])
    _guild_for(a, categories=[empty_text, empty_voice])
    asyncio.run(a._ensure_channels())
    assert empty_text.deleted is True and empty_voice.deleted is True


def test_nonempty_default_category_kept():
    a = DiscordAdapter("tok", [], _ALLOWED)
    survivor = _FakeChannel(1, "잡담")
    nonempty = _FakeCategory("채팅 채널", channels=[survivor])
    _guild_for(a, categories=[nonempty])
    asyncio.run(a._ensure_channels())
    assert nonempty.deleted is False  # 안 비었으면 보존


def test_bot_category_not_deleted_even_if_empty():
    # 봇 카테고리(스케쥴러 등)는 기본 이름 목록에 없어 삭제 대상 아님.
    a = DiscordAdapter("tok", [], _ALLOWED)
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    assert all(not c.deleted for c in guild.categories)  # 봇 카테고리 보존


# ---------------------------------------------------------------------------
# §4.3 프로젝트 목록 = Components V2 세로 1열(LayoutView) — 실측 요구
# ---------------------------------------------------------------------------


def _all_buttons(view):
    """LayoutView children(TextDisplay/ActionRow) 를 훑어 Button 만 평탄화."""
    out = []
    for it in view.children:
        if isinstance(it, discord.ui.ActionRow):
            out += [c for c in it.children if isinstance(c, discord.ui.Button)]
    return out


# ---------------------------------------------------------------------------
# 무회귀 골든 — 일상 회신의 렌더 결과를 문자 단위로 고정한다. 여기가 깨지면 폰에서 받는
# 회신(진행·완료·실패·확인·예약알림·목록·선택지)이 전부 바뀐 것이다.
# ---------------------------------------------------------------------------


def _payload(call):
    """스텁 튜플 → ("E", embed dict) | ("P", plain str) | ("V2", [자식 타입…])."""
    if call[0] in ("sendv", "editv"):
        return ("V2", [type(i).__name__ for i in call[-1].children])
    part = call[2] if call[0] == "send" else call[3]
    return ("E", part.to_dict()) if isinstance(part, discord.Embed) else ("P", part)


def _cids(call):
    view = call[3] if call[0] == "send" else call[4]
    return None if view is None else [c.custom_id for c in view.children]


_BASE = 15645517, 4116357, 15750747, 5793266  # 노랑·초록·빨강·블러플(값까지 고정)


# ---------------------------------------------------------------------------
# 시스템 소식 채널(표시명 알림 · tag 봇상태) — 🟢 기동 1회 · 🔌 재연결(10분 이상)
# ---------------------------------------------------------------------------
_STAMP_HEAD = r"^\[\d{4}-\d{2}-\d{2} (AM|PM) \d{2}:\d{2}\] "  # notice_stamp 머리(시각 비의존)


def test_special_system_category_has_one_channel_display_alert_tag_status():
    chans = discord_adapter._SPECIAL[discord_adapter._CAT_SYSTEM]
    assert chans == [("알림", "role", "봇상태")]  # 🔴 표시명 알림 · tag 봇상태(변경 금지)


def test_boot_notice_text_format():
    from datetime import datetime

    assert discord_adapter.boot_notice_text(datetime(2026, 10, 8, 9, 5)) == (
        "[2026-10-08 AM 09:05] 🟢 bridge On"
    )


def _at(ts):
    from datetime import datetime

    return datetime.fromtimestamp(ts)  # 어댑터와 같은 방식 — 로컬 타임존에 흔들리지 않는다


def test_reconnect_notice_texts_threshold():
    f = discord_adapter.reconnect_notice_texts
    since = 1_000_000.0
    assert f(since, since) == [] and f(since, since + 599) == []  # 10분 미만은 조용히
    for gap in (600, 1500):
        got = f(since, since + gap)
        assert got == [
            f"{bridge.notice_stamp(_at(since))} 🔴 bridge Off",
            f"{bridge.notice_stamp(_at(since + gap))} 🟢 bridge On",
        ]  # 순서: 끊긴 시각의 Off → 돌아온 시각의 On
        assert all("🔌" not in t and "동안" not in t for t in got)


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _status_adapter():
    clock = _Clock()
    a = DiscordAdapter("tok", [], _ALLOWED, clock=clock)
    posted: list[str] = []

    async def fake_post(text):
        posted.append(text)

    a._post_status = fake_post  # type: ignore[method-assign]
    return a, posted, clock


def test_boot_notice_only_once_across_reconnect_ready():
    a, posted, _clock = _status_adapter()
    asyncio.run(a._announce_ready())
    asyncio.run(a._announce_ready())  # 재연결로 on_ready 가 다시 와도 반복하지 않는다
    assert len(posted) == 1
    assert re.match(_STAMP_HEAD + "🟢 bridge On$", posted[0])


def test_reconnect_over_ten_minutes_reports_on_resumed():
    a, posted, clock = _status_adapter()
    a._ready.set()
    asyncio.run(a._client.on_disconnect())
    clock.t += 17 * 60
    asyncio.run(a._client.on_resumed())
    assert posted == [
        f"{bridge.notice_stamp(_at(1000.0))} 🔴 bridge Off",
        f"{bridge.notice_stamp(_at(1000.0 + 17 * 60))} 🟢 bridge On",
    ]
    asyncio.run(a._client.on_resumed())  # 소거됐다 — 두 번 말하지 않는다
    assert len(posted) == 2


def test_reconnect_under_ten_minutes_is_silent():
    a, posted, clock = _status_adapter()
    a._ready.set()
    asyncio.run(a._client.on_disconnect())
    clock.t += 9 * 60 + 59
    asyncio.run(a._client.on_resumed())
    assert posted == [] and a._disconnected_at is None


def test_reconnect_reported_on_second_ready_too():
    a, posted, clock = _status_adapter()
    a._ready.set()
    asyncio.run(a._announce_ready())  # 최초 ready → 기동 알림
    asyncio.run(a._client.on_disconnect())
    clock.t += 30 * 60
    asyncio.run(a._announce_ready())  # resume 대신 재-ready 가 온 경로
    # 기동 🟢 1건 + 재연결 Off/On 쌍 — 재-ready 가 기동 🟢 를 또 내지 않는다
    assert len(posted) == 3
    assert posted[0].endswith("🟢 bridge On") and re.match(_STAMP_HEAD, posted[0])
    assert posted[1] == f"{bridge.notice_stamp(_at(1000.0))} 🔴 bridge Off"
    assert posted[2] == f"{bridge.notice_stamp(_at(1000.0 + 30 * 60))} 🟢 bridge On"
    asyncio.run(a._announce_ready())  # 또 재-ready — 끊긴 시각이 소거됐으니 아무것도 안 나간다
    assert len(posted) == 3


def test_repeated_disconnects_keep_the_first_timestamp():
    a, posted, clock = _status_adapter()
    a._ready.set()
    asyncio.run(a._client.on_disconnect())
    clock.t += 8 * 60
    asyncio.run(a._client.on_disconnect())  # 재시도 중 반복 발화
    clock.t += 8 * 60
    asyncio.run(a._client.on_resumed())
    # Off 의 시각은 «처음» 끊긴 시각(1000)이지 재시도 중 반복 발화 시각이 아니다
    assert len(posted) == 2
    assert posted[0] == f"{bridge.notice_stamp(_at(1000.0))} 🔴 bridge Off"
    assert posted[1] == f"{bridge.notice_stamp(_at(1000.0 + 16 * 60))} 🟢 bridge On"


def test_disconnect_before_first_ready_is_ignored():
    a, posted, clock = _status_adapter()
    asyncio.run(a._client.on_disconnect())  # 접속 한 번도 못 한 상태
    clock.t += 60 * 60
    asyncio.run(a._announce_ready())
    assert len(posted) == 1 and re.match(_STAMP_HEAD + "🟢 ", posted[0])  # 🔴 Off 없음


def test_post_status_unmapped_does_not_raise(caplog):
    a = _adapter()
    with caplog.at_level("WARNING", logger="bridge"):
        asyncio.run(a._post_status("소식"))
    assert "미매핑" in caplog.text


def test_post_status_sends_masked_text_to_status_channel():
    a = DiscordAdapter("tok", ["SECRET"], _ALLOWED)
    a._channel_map = {88: ("role", "봇상태")}
    got = []

    async def fake_send(cid, payload, view):
        got.append((cid, payload, view))
        return 1

    a._send_coro = fake_send  # type: ignore[method-assign]
    asyncio.run(a._post_status("소식 SECRET"))
    assert got == [(88, "소식 ***", None)]


def test_post_status_send_failure_is_swallowed(caplog):
    a = _adapter()
    a._channel_map = {88: ("role", "봇상태")}

    async def boom(*_args):
        raise RuntimeError("down")

    a._send_coro = boom  # type: ignore[method-assign]
    with caplog.at_level("WARNING", logger="bridge"):
        asyncio.run(a._post_status("소식"))  # 예외 없음
    assert "전송 실패" in caplog.text
