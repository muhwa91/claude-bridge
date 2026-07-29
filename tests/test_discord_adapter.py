"""DiscordAdapter 계약 테스트(§5.2 — 디스코드 특화 단위).

이벤트루프 실구동(Gateway 접속)은 라이브 검증(0e) 몫이라 여기선 제외하고, 루프 없이 단위 검증
가능한 것만 다룬다: render_view(custom_id·스타일), fetch_file 보안(§2.4 CDN·확장자·크기·트래버설),
_message_event/_on_message 정규화·필터, _on_interaction defer 선행·custom_id 파싱·비허용 드롭,
send/edit 청킹·마스킹·버튼 말미(코루틴 경계는 _run 스텁), ack 멱등·맵 소비, close 안전성.

discord.py 미설치 환경(예: CI 최소셋)에서는 importorskip 으로 전체 스킵 → 236 코어 그린 불변.
"""

from __future__ import annotations

import asyncio
import urllib.error
from types import SimpleNamespace

import pytest

discord = pytest.importorskip("discord")  # 미설치면 이 파일 전체 스킵(코어 236 은 무영향)

import discord_adapter  # noqa: E402  (importorskip 뒤에 와야 함)
from adapter import Button, Event  # noqa: E402
from bridge import (  # noqa: E402
    HEADER_DONE,
    HEADER_FAIL,
    HEADER_NOTE,
    project_buttons,
    push_buttons,
)
from discord_adapter import DiscordAdapter, render_view  # noqa: E402

_ALLOWED = frozenset({777})


def _adapter(secrets=None, limit=discord_adapter.DISCORD_LIMIT):
    """접속하지 않는 어댑터(생성만) — poll() 을 부르지 않으면 Gateway 로 안 나간다."""
    return DiscordAdapter("tok", secrets if secrets is not None else [], _ALLOWED, limit=limit)


# ---------------------------------------------------------------------------
# render_view: Button → discord.ui.View (custom_id=encode_callback, 스타일 매핑, ≤100자)
# ---------------------------------------------------------------------------
def test_render_view_custom_id_and_style():
    view = render_view(
        [Button("✅ Push", "push", style="primary"), Button("❌ 취소", "x", style="danger")]
    )
    items = view.children
    assert [it.custom_id for it in items] == ["push", "x"]
    assert items[0].style == discord.ButtonStyle.primary
    assert items[1].style == discord.ButtonStyle.danger


def test_render_view_default_style_is_secondary():
    view = render_view([Button("데모", "p", "trading_info")])
    it = view.children[0]
    assert it.custom_id == "p:trading_info"  # encode_callback 직렬화
    assert it.style == discord.ButtonStyle.secondary
    assert it.label == "데모"


def test_render_view_choice_and_notify_custom_ids():
    from bridge import choice_buttons, notify_buttons

    v1 = render_view(notify_buttons("ti-open"))
    assert [c.custom_id for c in v1.children] == [
        "nb:ok:ti-open",
        "nb:done:ti-open",
        "nb:later:ti-open",
    ]
    v2 = render_view(choice_buttons(55, [("유지", "keep"), ("교체", "swap")]))
    assert [c.custom_id for c in v2.children] == ["c:55:0", "c:55:1", "c:55:other"]


def test_render_view_custom_id_within_discord_100_char_limit():
    # §1.3: DC custom_id ≤100자. id·name≤64 라 인코드 결과가 한도 안(캡은 오작동 없이 무시로 안전).
    from adapter import encode_callback

    for action, arg in (
        ("push", ""),
        ("x", ""),
        ("p", "x" * 64),
        ("nb:ok", "y" * 64),
        ("nb:later", "z" * 64),
        ("c", "999999:12"),
    ):
        assert len(encode_callback(action, arg)) <= discord_adapter._CUSTOM_ID_LIMIT


# ---------------------------------------------------------------------------
# fetch_file: §2.4 보안 계승(CDN 도메인·확장자·크기·트래버설)
# ---------------------------------------------------------------------------
class _FakeResp:
    def __init__(self, data=b"", headers=None):
        self._data = data
        self.headers = headers or {}

    def read(self, n=-1):
        return self._data if n is None or n < 0 else self._data[:n]

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


def _patch_urlopen(monkeypatch, resp):
    # fetch_file 은 리다이렉트 차단 opener(_NOREDIRECT_OPENER.open)를 쓴다(M-3) — 그걸 패치.
    monkeypatch.setattr(discord_adapter._NOREDIRECT_OPENER, "open", lambda *_a, **_k: resp)


_CDN = "https://cdn.discordapp.com/attachments/1/2/photo.png?ex=abc&is=def&hm=deadbeef"


def test_fetch_file_happy_writes_basename(monkeypatch, tmp_path):
    _patch_urlopen(monkeypatch, _FakeResp(b"\x89PNGdata"))
    dest = _adapter().fetch_file(_CDN, tmp_path)
    assert dest.name == "photo.png"  # 쿼리스트링 제거된 basename
    assert dest.parent == tmp_path
    assert dest.read_bytes() == b"\x89PNGdata"


def test_fetch_file_sends_user_agent(monkeypatch, tmp_path):
    # 함정 회귀 방지: 디스코드 CDN(Cloudflare)은 기본 UA(Python-urllib/*)를 403 차단(2026-07-22
    # 라이브 실측). Request 에 User-Agent 가 실려야 다운로드가 된다.
    seen = []
    _patch_urlopen(monkeypatch, _FakeResp(b"x"))  # 기본 패치 후 open 을 가로채 Request 캡처로 교체
    monkeypatch.setattr(
        discord_adapter._NOREDIRECT_OPENER,
        "open",
        lambda req, **_k: seen.append(req) or _FakeResp(b"x"),
    )
    _adapter().fetch_file(_CDN, tmp_path)
    assert seen and seen[0].get_header("User-agent")  # urllib 은 헤더 키를 이 형태로 정규화


def test_fetch_file_rejects_non_cdn_domain(monkeypatch, tmp_path):
    _patch_urlopen(monkeypatch, _FakeResp(b"x"))
    with pytest.raises(ValueError, match="도메인"):
        _adapter().fetch_file("https://evil.example.com/attachments/1/2/photo.png", tmp_path)


def test_fetch_file_rejects_http_scheme(monkeypatch, tmp_path):
    _patch_urlopen(monkeypatch, _FakeResp(b"x"))
    with pytest.raises(ValueError, match="도메인"):
        _adapter().fetch_file("http://cdn.discordapp.com/a/b/photo.png", tmp_path)


def test_fetch_file_rejects_bad_extension(monkeypatch, tmp_path):
    _patch_urlopen(monkeypatch, _FakeResp(b"x"))
    with pytest.raises(ValueError, match="확장자"):
        _adapter().fetch_file("https://cdn.discordapp.com/a/b/evil.gif", tmp_path)


def test_fetch_file_traversal_stays_basename(monkeypatch, tmp_path):
    _patch_urlopen(monkeypatch, _FakeResp(b"x"))
    # 경로에 ../ 가 있어도 basename 만 저장 → dest 밖으로 못 나감.
    dest = _adapter().fetch_file("https://media.discordapp.net/a/../../etc/evil.jpg", tmp_path)
    assert dest.name == "evil.jpg"
    assert dest.parent == tmp_path


def test_fetch_file_rejects_oversize_body(monkeypatch, tmp_path):
    monkeypatch.setattr(discord_adapter, "MAX_PHOTO_BYTES", 4)
    _patch_urlopen(monkeypatch, _FakeResp(b"toolongbody"))
    with pytest.raises(ValueError, match=r"10MB|상한"):
        _adapter().fetch_file(_CDN, tmp_path)


def test_fetch_file_rejects_oversize_content_length(monkeypatch, tmp_path):
    monkeypatch.setattr(discord_adapter, "MAX_PHOTO_BYTES", 4)
    _patch_urlopen(monkeypatch, _FakeResp(b"ok", headers={"Content-Length": "999"}))
    with pytest.raises(ValueError, match=r"10MB|상한"):
        _adapter().fetch_file(_CDN, tmp_path)


def test_fetch_file_rejects_redirect(monkeypatch, tmp_path):
    # M-3: CDN 이 3xx 로 내부주소(169.254.169.254)를 가리켜도 opener 가 추종 대신 HTTPError → 거부.
    def raise_302(*_a, **_k):
        raise urllib.error.HTTPError(_CDN, 302, "redirect blocked", {}, None)  # type: ignore[arg-type]

    monkeypatch.setattr(discord_adapter._NOREDIRECT_OPENER, "open", raise_302)
    with pytest.raises(urllib.error.HTTPError):
        _adapter().fetch_file(_CDN, tmp_path)


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
    reference=None,
):
    channel = SimpleNamespace(id=channel_id, name=channel_name)
    return SimpleNamespace(
        author=SimpleNamespace(id=user_id),
        channel=channel,
        content=content,
        id=msg_id,
        attachments=atts or [],
        reference=reference,  # §4.7 델타2: 답장 참조(None=일반 메시지)
    )


def test_message_event_text_normalization():
    ev = _adapter()._message_event(_msg(777, "etf_info 확인해줘"))
    assert ev.kind == "text"
    assert ev.channel_id == 100 and ev.user_id == 777
    assert ev.text == "etf_info 확인해줘" and ev.message_id == 5
    assert ev.project == "trading_info"  # 채널명 = 프로젝트 후보(0단계 매핑)


def test_message_event_photo_picks_image_attachment():
    att = SimpleNamespace(filename="toss.PNG", url=_CDN)
    ev = _adapter()._message_event(_msg(777, "MU", atts=[att]))
    assert ev.kind == "photo"
    assert ev.photo_ref == _CDN
    assert ev.text == "MU"


def test_message_event_dm_channel_project_none():
    channel = SimpleNamespace(id=9)  # name 속성 없음(DM)
    m = SimpleNamespace(
        author=SimpleNamespace(id=777),
        channel=channel,
        content="hi",
        id=1,
        attachments=[],
        reference=None,
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


def test_on_message_guest_channel_lets_unauth_through():
    # M-1: 비인가 게스트의 텍스트 질문이 선필터를 통과해 코어(_guest_bypass)까지 도달해야 동작.
    a = _adapter()
    a._channel_map = {600: ("role", "게스트질문")}
    asyncio.run(a._on_message(_msg(999, "파이썬이 뭐야", channel_id=600)))
    assert a._queue.qsize() == 1
    ev = a._queue.get_nowait()
    assert ev.channel_role == "게스트질문" and ev.user_id == 999


def test_on_interaction_guest_channel_button_still_dropped():
    # M-1: 게스트 채널 버튼은 선필터 그대로 드롭(코어 _guest_bypass 도 button 제외 — 대칭).
    a = _adapter()
    a._channel_map = {600: ("role", "게스트질문")}
    asyncio.run(a._on_interaction(_interaction(999, "clean:ok", channel_id=600)))
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
    inter = _interaction(777, "push", order=order)

    class _RecordQueue:
        def put(self, ev):
            order.append(("put", ev))

    a._queue = _RecordQueue()
    asyncio.run(a._on_interaction(inter))
    # defer 가 큐 적재보다 반드시 먼저(§2.3 3초 규약)
    assert order[0] == "defer"
    assert order[1][0] == "put"
    ev = order[1][1]
    assert ev.kind == "button" and ev.action == "push" and ev.callback_id == "9001"
    assert ev.channel_id == 100 and ev.message_id == 42 and ev.user_id == 777
    # interaction 이 ack 용으로 맵에 등록됨
    assert a._interactions["9001"] is inter


def test_on_interaction_parses_choice_custom_id():
    a = _adapter()
    asyncio.run(a._on_interaction(_interaction(777, "c:42:1")))
    ev = a._queue.get_nowait()
    assert ev.action == "c" and ev.action_arg == "42:1"


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


def test_ack_unknown_callback_is_noop():
    a = _adapter()
    ran = []
    a._run = lambda coro: ran.append(coro)  # type: ignore[assignment]
    a.ack("nope", "note")
    assert ran == []  # 맵에 없으면 followup 도 안 함


def test_ack_with_note_sends_followup_and_consumes_map():
    a = _adapter()
    inter = SimpleNamespace(name="i")
    a._interactions["9001"] = inter
    a._followup_coro = lambda interaction, note: ("followup", interaction, note)  # type: ignore[assignment]
    ran = []
    a._run = lambda coro: ran.append(coro)  # type: ignore[assignment]
    a.ack("9001", "확인")
    assert ran == [("followup", inter, "확인")]
    assert "9001" not in a._interactions  # 소비(맵 정리)


def test_ack_without_note_consumes_map_no_followup():
    a = _adapter()
    a._interactions["9001"] = SimpleNamespace()
    ran = []
    a._run = lambda coro: ran.append(coro)  # type: ignore[assignment]
    a.ack("9001")  # note 없음 → 이미 defer 됨, followup 안 함
    assert ran == []
    assert "9001" not in a._interactions


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


def test_status_color_matches_headers_and_leaders():
    sc = discord_adapter._status_color
    assert sc(f"{HEADER_DONE}\n\n끝") == discord_adapter._COLOR_DONE  # 초록
    assert sc(f"{HEADER_FAIL}\n\n실패") == discord_adapter._COLOR_FAIL  # 빨강
    assert sc(f"{HEADER_NOTE}\n\n확인") == discord_adapter._COLOR_INFO  # 블러플
    assert sc("🔄 작업 중") == discord_adapter._COLOR_WAIT  # 모든 진행 헤더 단일 문구 → 노랑
    assert sc("⏰ 개장 알림\n등락률 확인") == discord_adapter._COLOR_WAIT  # 예약 알림


def test_status_color_plain_returns_none():
    # 목록·도움말·짧은 회신은 매칭 안 됨 → plain(기존 무변경).
    assert discord_adapter._status_color("대상 프로젝트 3") is None
    assert discord_adapter._status_color("**사용법**\n...") is None
    assert discord_adapter._status_color("취소했습니다.") is None


def test_build_embed_title_strips_brackets_desc_is_body():
    embed, overflow = discord_adapter._build_embed(
        f"{HEADER_DONE}\n\nREADME 를 고쳤습니다.", discord_adapter._COLOR_DONE
    )
    assert embed.title == "✅처리완료"  # 대괄호 껍질 제거
    assert embed.description == "README 를 고쳤습니다."
    assert embed.color.value == discord_adapter._COLOR_DONE
    assert embed.author.name is None  # author 라인 제거(봇 계정명 중복)
    assert overflow == ""


def test_build_embed_overflow_beyond_4096():
    body = "x" * 5000
    embed, overflow = discord_adapter._build_embed(f"{HEADER_DONE}\n\n{body}", 0x1)
    assert len(embed.description) == 4096
    assert overflow == "x" * (5000 - 4096)


def test_send_status_header_renders_embed():
    a = _adapter()
    calls = _stub_calls(a, [111])
    a.send(100, f"{HEADER_DONE}\n\n완료 본문")
    payload = calls[0][2]  # ("send", cid, payload, view)
    assert isinstance(payload, discord.Embed)
    assert payload.color.value == discord_adapter._COLOR_DONE


def test_send_plain_stays_content_str():
    a = _adapter()
    calls = _stub_calls(a, [1])
    a.send(100, "대상 프로젝트 2")
    assert calls[0][2] == "대상 프로젝트 2"  # plain 그대로(str)


def test_edit_progress_to_done_transitions_embed_same_message():
    a = _adapter()
    calls = _stub_calls(a, [None])
    a.edit(100, 42, f"{HEADER_DONE}\n\n결과")
    # ("edit", cid, mid, payload, view)
    assert calls[0][0] == "edit" and calls[0][2] == 42
    assert isinstance(calls[0][3], discord.Embed)
    assert calls[0][3].color.value == discord_adapter._COLOR_DONE


def test_notify_status_renders_yellow_embed():
    # 예약 알림(⏰ 선두 이모지) → 진행/대기 노랑 임베드. #알림 채널 send 경로로 검증.
    a = _adapter()
    calls = _stub_calls(a, [1])
    a.send(999, "⏰ 개장 알림\n등락률 확인", [Button("✅ 확인시작", "nb:ok", "a")])
    payload = calls[0][2]
    assert isinstance(payload, discord.Embed)
    assert payload.color.value == discord_adapter._COLOR_WAIT


def test_embed_overflow_sends_followup_plain_chunk():
    a = _adapter()  # 기본 limit=2000 — 오버플로(104자)는 단일 후속 청크
    calls = _stub_calls(a, [111, None])
    a.send(100, f"{HEADER_DONE}\n\n" + "y" * 4200)  # desc 4096(고정) + 104 오버플로
    assert len(calls) == 2
    assert isinstance(calls[0][2], discord.Embed)  # 첫 파트 = 임베드
    assert isinstance(calls[1][2], str) and calls[1][2] == "y" * 104  # 오버플로 = 후속 plain


# ---------------------------------------------------------------------------
# 카드 렌더(🧩 다이제스트 — adapter.py 「Card 규약」). 공용 _build_embed 와 별도 경로.
# ---------------------------------------------------------------------------
_CARD_SPEC = {
    "author": "🧩 MCP축 · 1/2",
    "title": "claude-mem (⭐88,643)",
    "description": "**보류** · 세션 메모리 영속화.",
    "fields": [("👍 장점", "결손 축을 메움", True), ("🔧 적용", "훅 1건 — 20분", False)],
    "footer": "검토 24 · 기각 22",
    "color": 0xEEBB4D,
}


def test_build_card_embed_fills_every_slot():
    embed = discord_adapter.build_card_embed(_CARD_SPEC, [])
    assert embed.author.name == "🧩 MCP축 · 1/2"  # 축·순번은 author 슬롯(작은 회색 줄)
    assert embed.title == "claude-mem (⭐88,643)"
    assert embed.description == "**보류** · 세션 메모리 영속화."
    assert [(f.name, f.value, f.inline) for f in embed.fields] == _CARD_SPEC["fields"]
    assert embed.footer.text == "검토 24 · 기각 22"
    assert embed.color.value == 0xEEBB4D


def test_build_card_embed_two_layer_when_slots_missing():
    none_card = {
        "author": "🧩 에이전트 정의축",
        "title": "오늘 적용할 것 없음",
        "footer": "검토 12 · 기각 12",
    }
    embed = discord_adapter.build_card_embed(none_card, [])
    assert embed.description is None and embed.fields == []  # 본문·필드 없는 2층
    assert embed.color.value == discord_adapter._COLOR_INFO  # color 미지정 폴백


def test_build_card_embed_truncates_to_discord_limits():
    embed = discord_adapter.build_card_embed(
        {
            "author": "가" * 400,
            "title": "나" * 400,
            "fields": [("다" * 400, "라" * 2000, True)],
            "footer": "마" * 3000,
        },
        [],
    )
    assert len(embed.author.name) == discord_adapter._EMBED_AUTHOR_LIMIT
    assert len(embed.title) == discord_adapter._EMBED_TITLE_LIMIT
    assert len(embed.fields[0].name) == discord_adapter._EMBED_FIELD_NAME_LIMIT
    assert len(embed.fields[0].value) == discord_adapter._EMBED_FIELD_VALUE_LIMIT
    assert len(embed.footer.text) == discord_adapter._EMBED_FOOTER_LIMIT


def test_build_card_embed_masks_secrets_and_drops_empty_fields():
    embed = discord_adapter.build_card_embed(
        {"title": "T SECRET", "fields": [("👍 장점", "", True), ("", "값", True)]}, ["SECRET"]
    )
    assert embed.title == "T ***"  # 카드도 send/edit 과 같은 마스킹 문을 지난다
    assert embed.fields == []  # 이름·값이 빈 필드는 디스코드가 400 → 애초에 안 싣는다


def test_send_with_card_renders_single_embed_with_buttons():
    a = _adapter()
    calls = _stub_calls(a, [777])
    mid = a.send(100, "🧩 평문 폴백", [Button("📌 백로그", "od:add", "3")], card=_CARD_SPEC)
    assert mid == 777
    assert len(calls) == 1  # 카드는 항상 단일 메시지(청킹 없음)
    payload, view = calls[0][2], calls[0][3]
    assert isinstance(payload, discord.Embed) and payload.title == _CARD_SPEC["title"]
    assert [c.custom_id for c in view.children] == ["od:add:3"]


def test_edit_with_card_replaces_embed_and_drops_buttons():
    a = _adapter()
    calls = _stub_calls(a, [None])
    a.edit(100, 42, "🧩 평문 폴백", None, card=_CARD_SPEC)
    assert calls[0][0] == "edit" and calls[0][2] == 42
    assert isinstance(calls[0][3], discord.Embed)
    assert calls[0][4] is None  # 버튼 없음 → view=None(컴포넌트 제거)


def test_card_absent_keeps_existing_paths_untouched():
    # 무회귀: card 를 안 주면 상태 임베드·plain 경로가 그대로다(_build_embed 시그니처·판정 불변).
    a = _adapter()
    calls = _stub_calls(a, [1, 2])
    a.send(100, f"{HEADER_DONE}\n\n완료")
    a.send(100, "대상 프로젝트 2")
    assert isinstance(calls[0][2], discord.Embed) and calls[0][2].author.name is None
    assert calls[1][2] == "대상 프로젝트 2"
    # 🧩 는 상태색 대상이 아니다 — 카드는 card= 로 판정별 색을 싣고, 형식 이탈분은 임베드 없이
    # 평문 그대로 나간다(노랑은 진행·⏰예약알림 전용이라 폰에서 헷갈리지 않게).
    assert discord_adapter._status_color("🧩 MCP축 · x") is None


def test_digest_plain_fallback_is_plain_text_not_yellow_embed():
    """카드 파싱 실패분(card 없음)은 임베드 없이 평문 — ⏰ 예약알림 노랑으로 나가지 않는다."""
    a = _adapter()
    calls = _stub_calls(a, [1])
    a.send(100, "🧩 MCP축 owner/repo 차용\n적용 : 훅에 · 30분")
    assert calls[0][2] == "🧩 MCP축 owner/repo 차용\n적용 : 훅에 · 30분"  # str = plain


def test_digest_card_color_comes_from_core_spec():
    """카드 색은 코어가 판정별로 정해 넘긴 값 그대로(어댑터 상태색이 덮어쓰지 않는다)."""
    a = _adapter()
    calls = _stub_calls(a, [1])
    a.send(100, "🧩 평문 폴백", None, card={**_CARD_SPEC, "color": 0x3ECF85})
    assert calls[0][2].color.value == 0x3ECF85  # 즉시적용 초록


def test_render_view_success_and_secondary_styles():
    view = render_view(push_buttons())
    assert view.children[0].style == discord.ButtonStyle.success  # Push
    assert view.children[1].style == discord.ButtonStyle.secondary  # 취소


def test_message_event_fills_reply_to():
    # §4.7 델타2: message.reference.message_id → Event.reply_to.
    ref = SimpleNamespace(message_id=42)
    ev = _adapter()._message_event(_msg(777, "이어서", reference=ref))
    assert ev.reply_to == 42
    assert _adapter()._message_event(_msg(777, "일반")).reply_to is None


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
    m = {10: ("role", "알림"), 20: ("project", "etf_info")}
    discord_adapter.save_channel_map(p, m)
    assert discord_adapter.load_channel_map(p) == m


def test_load_channel_map_missing_and_corrupt(tmp_path):
    assert discord_adapter.load_channel_map(tmp_path / "none.json") == {}
    p = tmp_path / "bad.json"
    p.write_text("{bad", encoding="utf-8")
    assert discord_adapter.load_channel_map(p) == {}


def test_role_channel_reverse_lookup():
    a = _adapter()
    a._channel_map = {10: ("role", "알림"), 20: ("project", "etf_info"), 30: ("role", "봇상태")}
    assert a.role_channel("알림") == 10
    assert a.role_channel("봇상태") == 30
    assert a.role_channel("없는역할") is None


def test_project_channel_reverse_lookup():
    a = _adapter()
    a._channel_map = {10: ("role", "알림"), 20: ("project", "etf_info")}
    assert a.project_channel("etf_info") == 20  # 히트
    assert a.project_channel("알림") is None  # role 태그는 매칭 안 됨
    assert a.project_channel("없는프로젝트") is None  # 미스


def test_setup_channels_stores_names():
    a = _adapter()
    a.setup_channels(["a", "b"])
    assert a._project_names == ["a", "b"]


# ---------------------------------------------------------------------------
# clear_channel / _purge_coro — 채널 메시지 전체 삭제(파괴적, 부분 성공 허용)
# ---------------------------------------------------------------------------
def test_clear_channel_delegates_to_run_and_defaults_zero():
    a = _adapter()
    a._run = lambda coro: (coro.close(), 7)[1]  # type: ignore[assignment]  # 코루틴 닫고 카운트
    assert a.clear_channel(100) == 7
    a._run = lambda coro: (coro.close(), None)[1]  # type: ignore[assignment]  # 루프 미준비 폴백
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
    a._channel_map = {100: ("project", "etf_info"), 200: ("role", "간단처리")}
    ev_p = a._message_event(_msg(777, "hi", channel_id=100, channel_name="딴이름"))
    assert ev_p.project == "etf_info" and ev_p.channel_role is None  # 채널ID 매핑 우선
    ev_r = a._message_event(_msg(777, "hi", channel_id=200))
    assert ev_r.channel_role == "간단처리" and ev_r.project is None


def test_message_event_unmapped_falls_back_to_channel_name():
    a = _adapter()  # channel_map 비어있음
    ev = a._message_event(_msg(777, "hi", channel_id=100, channel_name="trading_info"))
    assert ev.project == "trading_info" and ev.channel_role is None


class _FakeChannel:
    def __init__(self, cid, name, position=0, reject_rename=False):
        self.id = cid
        self.name = name
        self.position = position
        self.deleted = False
        self.reject_rename = reject_rename  # 디스코드 이름 거부 시나리오
        self.renames = []  # edit(name=…) 시도 기록

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
    a.setup_channels(["etf_info", "trading_info"])
    guild = _FakeGuild()
    a._client = SimpleNamespace(guilds=[guild])  # type: ignore[assignment]
    asyncio.run(a._ensure_channels())
    tags = set(a._channel_map.values())
    assert {
        ("role", "간단처리"),
        ("role", "데이터분석"),
        ("role", "알림"),
        ("role", "봇상태"),
    } <= tags
    assert {("project", "etf_info"), ("project", "trading_info")} <= tags
    assert any(
        topic == discord_adapter._DATA_TOPIC for _n, topic in guild.created
    )  # 데이터-분석 토픽
    assert a.role_channel("알림") is not None and a.role_channel("봇상태") is not None
    assert cm.exists()  # 영속
    assert discord_adapter.load_channel_map(cm) == a._channel_map


def test_ensure_channels_reuses_existing_by_canon():
    # 재사용 = channelID + _canon(언더스코어/하이픈 무시) — h-security-sheet 로 저장돼도 매칭.
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["H_security_sheet"])
    existing = _FakeChannel(777, "h-security-sheet")
    guild = _FakeGuild(text_channels=[existing])
    a._client = SimpleNamespace(guilds=[guild])  # type: ignore[assignment]
    asyncio.run(a._ensure_channels())
    assert a._channel_map[777] == ("project", "H_security_sheet")  # 기존 채널ID 재사용
    assert not any(discord_adapter._canon(n) == "hsecuritysheet" for n, _ in guild.created)


def test_concurrent_on_ready_no_duplicate(monkeypatch):
    # F1: 첫 셋업 중 reconnect 로 on_ready 2회 겹쳐도 _setup_lock 이 직렬화 → 중복 생성 없음.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["etf_info"])
    guild = _guild_for(a)

    async def two_on_ready():
        await asyncio.gather(a._ensure_channels(), a._ensure_channels())

    asyncio.run(two_on_ready())
    # 카테고리 7개(간단처리·프로젝트·데이터분석·스케쥴러·시스템·질문·PlayList, 중복 X).
    assert len(guild.categories) == 7
    assert [n for n, _ in guild.created].count("etf_info") == 1


def test_ensure_channels_no_guild_skips(tmp_path):
    a = DiscordAdapter("tok", [], _ALLOWED, channel_map_file=tmp_path / "cm.json")
    a.setup_channels(["etf_info"])
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


def test_ensure_channels_channel_create_failure_skips_but_maps_rest(monkeypatch):
    # "실패는 로그+계속"(§4.4) 회귀 잠금: 한 채널 생성이 디스코드 예외로 실패해도 전체 setup 이
    # 중단되지 않고 나머지 채널은 정상 매핑된다(권한 오류 1건이 전체 라우팅을 지우지 않게).
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})

    class _RejectingGuild(_FakeGuild):
        async def create_text_channel(self, name, **kwargs):
            if name == "알림":  # 특정 채널만 생성 거부(권한 없음 모사)
                raise discord.DiscordException("forbidden")
            return await super().create_text_channel(name, **kwargs)

    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["etf_info"])
    guild = _RejectingGuild()
    a._client = SimpleNamespace(guilds=[guild])  # type: ignore[assignment]
    asyncio.run(a._ensure_channels())  # 예외로 안 죽음
    tags = set(a._channel_map.values())
    assert ("role", "알림") not in tags  # 실패한 채널은 미매핑(스킵)
    # 형제 채널·프로젝트·다른 특수채널은 정상 매핑(부분 실패가 전체를 무너뜨리지 않음)
    assert {("role", "간단처리"), ("role", "봇상태"), ("project", "etf_info")} <= tags


# ---------------------------------------------------------------------------
# ① 후속: 프로젝트 채널명=한글 라벨(리네임)·음성 PlayList·카테고리 순서·기본 #일반 삭제·멱등
# ---------------------------------------------------------------------------


def _guild_for(a, **kw):
    guild = _FakeGuild(**kw)
    a._client = SimpleNamespace(guilds=[guild])  # type: ignore[assignment]
    return guild


def test_project_channel_renamed_to_label_via_map(monkeypatch):
    # 기존 폴더명 채널(맵에 등록) → 라벨 붙여쓰기로 리네임. 매핑값은 폴더명 원문 불변, 재생성 없음.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {"trading_info": "주식 모니터링"})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["trading_info"])
    a._channel_map = {500: ("project", "trading_info")}  # 라이브가 만든 폴더명 채널
    ch = _FakeChannel(500, "trading_info")
    guild = _guild_for(a, text_channels=[ch])
    asyncio.run(a._ensure_channels())
    assert ch.renames == ["주식모니터링"]  # 공백 제거 붙여쓰기명으로 리네임
    assert a._channel_map[500] == ("project", "trading_info")  # 매핑=폴더명 불변(라우팅)
    # 재생성 X — 정확명 비교(부분일치 '주식' 금지: 미국주식 무관)
    assert not any(n in ("주식모니터링", "trading_info") for n, _ in guild.created)


def test_label_rename_idempotent_second_run(monkeypatch):
    # 이미 붙여쓰기명이면 재기동해도 리네임 안 함(정확 일치 skip — 진짜 멱등).
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {"trading_info": "주식 모니터링"})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["trading_info"])
    a._channel_map = {500: ("project", "trading_info")}
    ch = _FakeChannel(500, "주식모니터링")  # 이미 붙여쓰기 저장형
    _guild_for(a, text_channels=[ch])
    asyncio.run(a._ensure_channels())
    assert ch.renames == []  # 정확 일치 → skip


def test_hyphen_form_force_renamed_to_joined(monkeypatch):
    # ⚠️ 멱등 함정 회귀: 하이픈형 '주식-모니터링'은 canon 은 같아도 정확 이름이 달라 강제 리네임.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {"trading_info": "주식 모니터링"})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["trading_info"])
    a._channel_map = {500: ("project", "trading_info")}
    ch = _FakeChannel(500, "주식-모니터링")  # 구 하이픈형(canon 동일)
    _guild_for(a, text_channels=[ch])
    asyncio.run(a._ensure_channels())
    assert ch.renames == ["주식모니터링"]  # canon 같아도 정확 비교로 리네임 실행


def test_special_channel_names_are_joined(monkeypatch):
    # 특수 채널명 붙여쓰기(하이픈 없음): 간단처리·데이터분석·알림·봇상태. (빈이름 폐기 — 정상명)
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    created = {n for n, _ in guild.created}
    assert {"간단처리", "데이터분석", "알림", "봇상태"} <= created
    assert not any("-" in n for n in created)  # 하이픈 없음


def test_simple_channel_name_idempotent(monkeypatch):
    # 간단처리 텍스트 채널이 이미 정상명이면 재기동해도 리네임 안 함(정확 일치 skip).
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    simple = _FakeChannel(700, "간단처리")
    a._channel_map = {700: ("role", "간단처리")}
    _guild_for(a, text_channels=[simple])
    asyncio.run(a._ensure_channels())
    assert simple.renames == []  # 이미 목표명 → skip
    assert a._channel_map[700] == ("role", "간단처리")  # 라우팅 태그 불변


def test_rename_rejected_keeps_mapping(monkeypatch):
    # 디스코드가 리네임 거부(400 등) → 기존명 보존하되 channel_map 매핑은 유지(라우팅 안 깨짐).
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    simple = _FakeChannel(700, "구이름", reject_rename=True)  # 목표명과 달라 리네임 시도됨
    a._channel_map = {700: ("role", "간단처리")}
    _guild_for(a, text_channels=[simple])
    asyncio.run(a._ensure_channels())
    assert simple.renames == ["간단처리"] and simple.name == "구이름"  # 시도했으나 거부→기존명 보존
    assert a._channel_map[700] == ("role", "간단처리")  # 매핑 유지(라우팅 OK)


def test_label_fallback_to_folder_when_no_label(monkeypatch):
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["etf_info"])
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    # 라벨 없음 → 폴더명으로 생성(매핑 tag=폴더)
    assert ("project", "etf_info") in set(a._channel_map.values())
    assert any(n == "etf_info" for n, _ in guild.created)


def test_voice_playlist_renames_default_general(monkeypatch):
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    default_voice = _FakeChannel(900, "일반")
    guild = _guild_for(a, voice_channels=[default_voice])
    asyncio.run(a._ensure_channels())
    assert default_voice.renames == ["PlayList"]  # 기본음성 → PlayList 리네임(삭제+생성 아님)
    assert guild.voice_created == []  # 새 음성 생성 안 함
    assert a._channel_map[900] == ("role", "playlist")


def test_special_registers_guest_role_in_question_category(monkeypatch):
    # 게스트질문 role 은 ❓ 질문 카테고리, 간단처리 role 은 🗂️ 간단처리(게스트 뺌·무손상).
    assert ("게스트질문", "role", "게스트질문") in discord_adapter._SPECIAL[
        discord_adapter._CAT_QUESTION
    ]
    assert discord_adapter._SPECIAL[discord_adapter._CAT_SIMPLE] == [
        ("간단처리", "role", "간단처리")
    ]
    # 카테고리 순서: 시스템 < 질문 < PlayList.
    order = discord_adapter._CAT_ORDER
    assert (
        order.index(discord_adapter._CAT_SYSTEM)
        < order.index(discord_adapter._CAT_QUESTION)
        < order.index(discord_adapter._CAT_VOICE)
    )
    # 봇 재기동(_ensure_channels) 시 채널 자동 생성·매핑되는지(role tag 불변).
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    _guild_for(a)
    asyncio.run(a._ensure_channels())
    assert ("role", "게스트질문") in set(a._channel_map.values())


def test_voice_playlist_created_when_no_default(monkeypatch):
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    assert guild.voice_created == ["PlayList"]
    assert ("role", "playlist") in set(a._channel_map.values())


def test_voice_playlist_idempotent_when_named(monkeypatch):
    # 이미 'PlayList' 이면 재기동해도 리네임 안 함(정확 일치 skip, 실패 재시도 없음).
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    existing = _FakeChannel(901, "PlayList")
    a._channel_map = {901: ("role", "playlist")}
    guild = _guild_for(a, voice_channels=[existing])
    asyncio.run(a._ensure_channels())
    assert guild.voice_created == [] and existing.renames == []


def test_categories_ordered(monkeypatch):
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["etf_info"])
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    order = {discord_adapter._cat_core(c.name): c.position for c in guild.categories}
    assert order["간단처리"] == 0
    assert order["프로젝트"] == 1
    assert order["데이터분석"] == 2
    assert order["스케쥴러"] == 3  # 🗓️ 스케쥴러 (데이터분석 아래·시스템 위)
    assert order["시스템"] == 4
    assert order["질문"] == 5  # ❓ 질문 (시스템 아래·PlayList 위)
    assert order["playlist"] == 6  # 🎵 PlayList


def test_scheduler_category_in_order_between_data_and_system():
    # 🗓️ 스케쥴러 = _CAT_ORDER index 3(데이터분석 아래·시스템 위).
    order = discord_adapter._CAT_ORDER
    assert order.index(discord_adapter._CAT_SCHED) == 3
    assert order[2] == discord_adapter._CAT_DATA
    assert order[4] == discord_adapter._CAT_SYSTEM
    assert discord_adapter._CAT_ALIASES[discord_adapter._CAT_SCHED] == ["스케쥴러"]


def test_scheduler_special_has_two_role_channels():
    assert discord_adapter._SPECIAL[discord_adapter._CAT_SCHED] == [
        ("미국주식", "role", "미국주식"),
        ("오픈소스", "role", "오픈소스"),
    ]


def test_scheduler_channels_created_and_mapped(monkeypatch):
    # 새 카테고리 🗓️ 스케쥴러 아래에 미국주식·오픈소스 role 채널 생성 + channel_map 매핑.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    tags = set(a._channel_map.values())
    assert ("role", "미국주식") in tags and ("role", "오픈소스") in tags
    created = {n for n, _ in guild.created}
    assert {"미국주식", "오픈소스"} <= created
    assert a.role_channel("미국주식") is not None and a.role_channel("오픈소스") is not None
    assert any(discord_adapter._cat_core(c.name) == "스케쥴러" for c in guild.categories)


def test_categories_created_with_emoji(monkeypatch):
    # #1: 카테고리 헤더에 이모지 표시명으로 생성.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    guild = _guild_for(a)
    asyncio.run(a._ensure_channels())
    names = {c.name for c in guild.categories}
    assert {"🗂️ 간단처리", "📁 프로젝트", "📊 데이터분석", "⚙️ 시스템", "🎵 PlayList"} <= names


def test_existing_category_renamed_to_emoji_idempotent(monkeypatch):
    # 기존 '간단처리'(이모지 없음) → '🗂️ 간단처리' 로 rename. 재기동 시 이미 이모지형이면 skip.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    plain = _FakeCategory("간단처리")
    already = _FakeCategory("📊 데이터분석")  # 이미 이모지형
    _guild_for(a, categories=[plain, already])
    asyncio.run(a._ensure_channels())
    assert plain.renames == ["🗂️ 간단처리"]  # 코어명 매칭 → 이모지 rename
    assert already.renames == []  # 정확 일치 → skip(멱등)


def test_voice_category_renamed_from_old_name(monkeypatch):
    # 음성 카테고리 이전 이름 '음성' → '🎵 PlayList' 로 이관(별칭 매칭).
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    old_voice = _FakeCategory("음성")
    _guild_for(a, categories=[old_voice])
    asyncio.run(a._ensure_channels())
    assert old_voice.renames == ["🎵 PlayList"]


def test_project_order_h_channels_first(monkeypatch):
    # #2: pdf_restyler·H_security_sheet 를 맨 위. 정본 순서로 재정렬.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["trading_info", "pdf_restyler"])  # 입력 순서 무관
    trading = _FakeChannel(10, "trading_info", position=0)
    pdf = _FakeChannel(20, "pdf_restyler", position=1)
    a._channel_map = {10: ("project", "trading_info"), 20: ("project", "pdf_restyler")}
    _guild_for(a, text_channels=[trading, pdf])
    asyncio.run(a._ensure_channels())
    # 정본: pdf_restyler(0) → trading_info(1)
    assert pdf.position == 0 and trading.position == 1


def test_default_general_text_deleted(monkeypatch):
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    general = _FakeChannel(950, "일반")  # 기본 텍스트(맵에 없음 = 봇 생성 아님)
    _guild_for(a, text_channels=[general])
    asyncio.run(a._ensure_channels())
    assert general.deleted is True


def test_default_general_kept_if_bot_channel(monkeypatch):
    # 안전장치: 이름이 '일반'이어도 봇이 만든(new_map 등록) 채널이면 삭제 안 함.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {"weird": "일반"})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["weird"])
    a._channel_map = {950: ("project", "weird")}
    botch = _FakeChannel(950, "일반")  # 봇이 라벨 '일반'으로 만든 채널(맵에 있음)
    _guild_for(a, text_channels=[botch])
    asyncio.run(a._ensure_channels())
    assert botch.deleted is False


def test_project_channels_ordered_by_labels_json(monkeypatch):
    # #4: 프로젝트 채널 내부 순서 = project_labels.json 순(위치 뒤섞인 기존 채널 재정렬).
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {"trading_info": "A", "etf_info": "B"})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["etf_info", "trading_info"])  # list_projects 알파벳순(정본 아님)
    # 기존 채널이 역순 position(etf=0, trading=1) — 재정렬 필요
    etf = _FakeChannel(10, "b", position=0)
    trading = _FakeChannel(20, "a", position=1)
    a._channel_map = {10: ("project", "etf_info"), 20: ("project", "trading_info")}
    _guild_for(a, text_channels=[etf, trading])
    asyncio.run(a._ensure_channels())
    # labels.json 순 = trading_info→etf_info → position 재설정(trading=0, etf=1)
    assert trading.position == 0 and etf.position == 1


def test_project_order_idempotent_when_already_sorted(monkeypatch):
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {"trading_info": "A", "etf_info": "B"})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels(["etf_info", "trading_info"])
    trading = _FakeChannel(20, "a", position=0)  # 이미 정렬됨
    etf = _FakeChannel(10, "b", position=1)
    a._channel_map = {10: ("project", "etf_info"), 20: ("project", "trading_info")}
    _guild_for(a, text_channels=[trading, etf])
    asyncio.run(a._ensure_channels())
    assert trading.renames == [] and etf.renames == []  # 이름도 이미 맞음
    # position edit 없이 순서 그대로(멱등)
    assert trading.position == 0 and etf.position == 1


def test_empty_default_categories_deleted(monkeypatch):
    # #5: 비어있는 기본 카테고리(채팅 채널/음성 채널)만 삭제 — 이중 가드.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    empty_text = _FakeCategory("채팅 채널", channels=[])
    empty_voice = _FakeCategory("Voice Channels", channels=[])
    _guild_for(a, categories=[empty_text, empty_voice])
    asyncio.run(a._ensure_channels())
    assert empty_text.deleted is True and empty_voice.deleted is True


def test_nonempty_default_category_kept(monkeypatch):
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
    survivor = _FakeChannel(1, "잡담")
    nonempty = _FakeCategory("채팅 채널", channels=[survivor])
    _guild_for(a, categories=[nonempty])
    asyncio.run(a._ensure_channels())
    assert nonempty.deleted is False  # 안 비었으면 보존


def test_bot_category_not_deleted_even_if_empty(monkeypatch):
    # 봇 카테고리(간단처리 등)는 기본 이름 목록에 없어 삭제 대상 아님.
    monkeypatch.setattr(discord_adapter, "PROJECT_LABELS", {})
    a = DiscordAdapter("tok", [], _ALLOWED)
    a.setup_channels([])
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


def test_render_project_view_is_vertical_one_per_row():
    names = ["a", "b", "c", "d", "e", "f", "g"]  # 7개 — classic View 5행 한도 초과
    view = discord_adapter.render_project_view("대상 프로젝트 7", project_buttons(names))
    rows = [it for it in view.children if isinstance(it, discord.ui.ActionRow)]
    assert len(rows) == 7  # 세로 1열: 프로젝트당 ActionRow 1개
    assert all(len(r.children) == 1 for r in rows)  # 행마다 버튼 1개
    btns = _all_buttons(view)
    assert [b.custom_id for b in btns] == [f"p:{n}" for n in names]
    assert all(b.style == discord.ButtonStyle.primary for b in btns)  # 다크 대비 블러플


def test_render_project_view_empty_header_buttons_only():
    # /projects 는 헤더 텍스트 없음(빈 header) → TextDisplay 생략, 버튼만.
    view = discord_adapter.render_project_view("", project_buttons(["a", "b"]))
    assert not any(isinstance(it, discord.ui.TextDisplay) for it in view.children)
    assert len(_all_buttons(view)) == 2


def test_render_project_view_nonempty_header_keeps_textdisplay():
    # 못 찾음 등 의미 있는 안내는 TextDisplay 로 유지(버튼 + 사유).
    view = discord_adapter.render_project_view(
        "'x' 프로젝트를 찾지 못했습니다.", project_buttons(["a"])
    )
    assert any(isinstance(it, discord.ui.TextDisplay) for it in view.children)


def test_is_vertical_list_detects_p_and_c():
    from bridge import choice_buttons

    assert discord_adapter._is_vertical_list(project_buttons(["a", "b"]))  # p:
    assert discord_adapter._is_vertical_list(choice_buttons(55, [("유지", "keep")]))  # c:
    assert not discord_adapter._is_vertical_list(push_buttons())  # push/x → 가로 유지
    assert not discord_adapter._is_vertical_list([])
    assert not discord_adapter._is_vertical_list(None)


def test_send_project_list_routes_to_v2_view_coro():
    a = _adapter()
    calls = []
    a._send_view_coro = lambda cid, view: ("v2", cid, view)  # type: ignore[assignment]
    a._send_coro = lambda cid, body, view: ("classic", cid, body, view)  # type: ignore[assignment]

    def fake_run(coro):
        calls.append(coro)
        return 321

    a._run = fake_run  # type: ignore[assignment]
    mid = a.send(100, "대상 프로젝트 2", project_buttons(["a", "b"]))
    assert mid == 321
    assert calls[0][0] == "v2"  # 클래식 _emit 경로 아님
    view = calls[0][2]
    assert isinstance(view, discord.ui.LayoutView)


def test_send_non_project_buttons_still_classic():
    a = _adapter()
    calls = _stub_calls(a, [111])
    a.send(100, HEADER_NOTE + "\n\npush?", push_buttons())  # p: 아님 → classic View 경로
    assert calls[0][0] == "send"  # _send_coro(classic) 사용


def test_send_choice_buttons_routes_to_v2_and_tracks_id():
    from bridge import choice_buttons

    a = _adapter()
    calls = []
    a._send_view_coro = lambda cid, view: ("v2", cid, view)  # type: ignore[assignment]
    a._send_coro = lambda cid, body, view: ("classic", cid, body, view)  # type: ignore[assignment]

    def fake_run(coro):
        calls.append(coro)
        return 900

    a._run = fake_run  # type: ignore[assignment]
    mid = a.send(100, "↳ 택일 하세요:", choice_buttons(0, [("유지", "keep"), ("교체", "swap")]))
    assert mid == 900
    assert calls[0][0] == "v2"  # 선택지(c:)도 세로 V2 경로
    assert isinstance(calls[0][2], discord.ui.LayoutView)
    assert 900 in a._v2_messages  # 이후 edit 이 V2 로 유지되도록 id 기록


def test_edit_tracked_v2_message_routes_to_v2_view_coro():
    from bridge import choice_buttons

    a = _adapter()
    a._v2_messages.add(900)  # send 가 V2 로 만든 선택지 메시지라고 가정
    calls = []
    a._edit_view_coro = lambda cid, mid, view: ("v2edit", cid, mid, view)  # type: ignore[assignment]
    a._edit_coro = lambda cid, mid, body, view: ("classicedit", cid, mid, body, view)  # type: ignore[assignment]
    a._run = lambda coro: calls.append(coro)  # type: ignore[assignment]
    # 실제 id 로 버튼 갱신(V2→V2)
    a.edit(100, 900, "↳ 택일 하세요:", choice_buttons(900, [("유지", "keep")]))
    assert calls[0][0] == "v2edit"
    assert isinstance(calls[0][3], discord.ui.LayoutView)
    # 버튼 제거(선택 확정) — buttons=None 도 V2 유지(TextDisplay 만, 400 방지)
    calls.clear()
    a.edit(100, 900, "선택: 유지")
    assert calls[0][0] == "v2edit"
    assert isinstance(calls[0][3], discord.ui.LayoutView)


def test_edit_untracked_message_stays_classic():
    a = _adapter()
    calls = _stub_calls(a, [None])
    a.edit(100, 42, "짧음")  # _v2_messages 에 없음 → classic 경로
    assert calls[0][0] == "edit"  # _edit_coro(classic)


# ---------------------------------------------------------------------------
# 무회귀 골든 — card 미지정(= 카드 도입 전 모든 메시지)의 렌더 결과를 문자 단위로 고정한다.
# `card` 인자 추가·build_card_embed 신설이 공용 _build_embed/_status_color 경로를 건드리지
# 않았음을 증명한다(HEAD 실행 결과와 대조해 동일 확인, 2026-07-27 QA). 여기가 깨지면 폰에서
# 받는 일상 회신(진행·완료·실패·확인·예약알림·목록·선택지)이 전부 바뀐 것이다.
# ---------------------------------------------------------------------------
def _stub_all(adapter, ids):
    """_stub_calls + V2(LayoutView) 코루틴까지 — 목록·선택지가 실코루틴을 만들지 않게."""
    calls = _stub_calls(adapter, ids)
    adapter._send_view_coro = lambda cid, view: ("sendv", cid, view)  # type: ignore[assignment]
    adapter._edit_view_coro = lambda cid, mid, view: ("editv", cid, mid, view)  # type: ignore[assignment]
    return calls


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


def _kinds():
    from bridge import HEADER_CHOICE, choice_buttons, notify_buttons

    wait, done, fail, info = _BASE
    return [
        (
            "🔄 작업 중\n\n프로젝트: x",
            push_buttons(),
            (
                "E",
                {
                    "flags": 0,
                    "color": wait,
                    "type": "rich",
                    "description": "프로젝트: x",
                    "title": "🔄 작업 중",
                },
            ),
            ["push", "x"],
        ),
        (
            f"{HEADER_DONE}\n\n다 됐습니다 SECRET",
            None,
            (
                "E",
                {
                    "flags": 0,
                    "color": done,
                    "type": "rich",
                    "description": "다 됐습니다 ***",
                    "title": "✅처리완료",
                },
            ),
            None,
        ),
        (
            f"{HEADER_FAIL}\n\n오류",
            None,
            (
                "E",
                {
                    "flags": 0,
                    "color": fail,
                    "type": "rich",
                    "description": "오류",
                    "title": "❌처리실패",
                },
            ),
            None,
        ),
        (
            f"{HEADER_NOTE}\n\n뭐 할까요",
            None,
            (
                "E",
                {
                    "flags": 0,
                    "color": info,
                    "type": "rich",
                    "description": "뭐 할까요",
                    "title": "📌추가 확인사항",
                },
            ),
            None,
        ),
        (
            "⏰ 알림\n\n장 열림",
            notify_buttons("ti-open"),
            (
                "E",
                {
                    "flags": 0,
                    "color": wait,
                    "type": "rich",
                    "description": "장 열림",
                    "title": "⏰ 알림",
                },
            ),
            ["nb:ok:ti-open", "nb:done:ti-open", "nb:later:ti-open"],
        ),
        (
            f"{HEADER_CHOICE}\n\n고르세요",
            None,
            (
                "E",
                {
                    "flags": 0,
                    "color": info,
                    "type": "rich",
                    "description": "고르세요",
                    "title": "❓선택",
                },
            ),
            None,
        ),
        ("대상 프로젝트 3", None, ("P", "대상 프로젝트 3"), None),
        ("", None, ("P", "(빈 응답)"), None),
        (
            "대상 프로젝트",
            project_buttons(["a", "b"]),
            ("V2", ["TextDisplay", "ActionRow", "ActionRow"]),
            None,
        ),
        (
            "골라",
            choice_buttons(55, [("유지", "k"), ("교체", "s")]),
            ("V2", ["TextDisplay", "ActionRow", "ActionRow", "ActionRow"]),
            None,
        ),
    ]


@pytest.mark.parametrize(("text", "buttons", "expect", "cids"), _kinds())
def test_send_without_card_is_byte_identical_baseline(text, buttons, expect, cids):
    a = _adapter(secrets=["SECRET"])
    calls = _stub_all(a, [1, 2])
    a.send(100, text, buttons)
    assert len(calls) == 1 and _payload(calls[0]) == expect
    if expect[0] != "V2":
        assert _cids(calls[0]) == cids


@pytest.mark.parametrize(("text", "buttons", "expect", "cids"), _kinds())
def test_send_with_card_none_equals_omitted(text, buttons, expect, cids):
    """card=None 은 인자를 아예 안 준 것과 완전히 같아야 한다(기존 호출부 전부 이 경로)."""
    a = _adapter(secrets=["SECRET"])
    calls = _stub_all(a, [1, 2])
    a.send(100, text, buttons, card=None)
    assert [_payload(c) for c in calls] == [expect]
    if expect[0] != "V2":
        assert _cids(calls[0]) == cids


@pytest.mark.parametrize(("text", "buttons", "expect", "_cid"), _kinds())
def test_edit_without_card_is_byte_identical_baseline(text, buttons, expect, _cid):
    a = _adapter(secrets=["SECRET"])
    calls = _stub_all(a, [None, None])
    a.edit(100, 42, text, buttons)
    got = _payload(calls[0])
    if expect[0] == "V2":  # 목록·선택지는 send 가 V2 로 만들었을 때만 V2 편집(미추적 → classic)
        assert got[0] in ("P", "E")
    else:
        assert got == expect


def test_send_overflow_without_card_still_splits():
    """4096 초과 본문의 오버플로 후속 청크 = 종전 그대로(카드 경로가 청킹을 건드리지 않았다)."""
    a = _adapter()
    calls = _stub_calls(a, [1, 2])
    a.send(100, f"{HEADER_DONE}\n\n" + "y" * 4200)
    assert len(calls) == 2
    assert isinstance(calls[0][2], discord.Embed) and len(calls[0][2].description) == 4096
    assert calls[1][2] == "y" * 104


# ── 카드 한도·마스킹 보강(기존 테스트 미커버분) ─────────────────────────────
def test_build_card_embed_caps_description_and_field_count():
    embed = discord_adapter.build_card_embed(
        {
            "description": "설" * 5000,
            "fields": [(f"n{i}", f"v{i}", True) for i in range(40)],
        },
        [],
    )
    assert len(embed.description) == discord_adapter._EMBED_DESC_LIMIT  # 4096
    assert len(embed.fields) == discord_adapter._EMBED_FIELDS_MAX  # 25개 초과분 절단
    assert [f.name for f in embed.fields] == [f"n{i}" for i in range(25)]  # 앞에서부터


def test_build_card_embed_announces_omitted_fields_in_footer():
    """조용한 절단 금지 — 미국주식 카드면 마지막 두 필드(한국·섹터)가 소리 없이 사라진다.
    이 모듈의 원칙("조용히 죽는 것보다 안 쓴다고 말하고 죽는 것이 낫다")과 어긋난다."""
    embed = discord_adapter.build_card_embed(
        {"footer": "출처", "fields": [(f"n{i}", "값" * 1000, False) for i in range(10)]}, []
    )
    assert "생략" in embed.footer.text and "출처" in embed.footer.text
    assert len(embed.footer.text) <= discord_adapter._OMIT_NOTE_MAXLEN + len("출처") + 3


def test_build_card_embed_clamps_description_to_total_budget():
    # 슬롯별 한도만 지킨 조합(title 256 + desc 4096 + author 256 + footer 2048 = 6656)은
    # **필드가 0개여도** 6000 을 넘겨 400 이 난다.
    embed = discord_adapter.build_card_embed(
        {
            "title": "제" * 256,
            "author": "저" * 256,
            "footer": "꼬" * 2048,
            "description": "설" * 4096,
            "fields": [],
        },
        [],
    )
    total = (
        len(embed.title) + len(embed.description) + len(embed.author.name) + len(embed.footer.text)
    )
    assert total <= discord_adapter._EMBED_TOTAL_LIMIT


def test_build_card_embed_drops_fields_past_total_limit():
    """슬롯별 한도를 다 지켜도 **합계 6000 을 넘으면 디스코드가 400 으로 거부**해 메시지 자체가
    사라진다. 호출자가 둘(오픈소스 5x1000 · 미국주식 8x700)이고 산식이 달라 누적으로도 센다.
    넘치는 필드는 버리되 앞쪽은 살린다 — 한 필드를 잃는 게 카드 전체를 잃는 것보다 낫다.
    """
    embed = discord_adapter.build_card_embed(
        {"title": "T", "fields": [(f"n{i}", "값" * 1000, False) for i in range(10)]}, []
    )
    total = len(embed.title or "") + sum(len(f.name) + len(f.value) for f in embed.fields)
    assert total <= discord_adapter._EMBED_TOTAL_LIMIT
    assert 0 < len(embed.fields) < 10  # 일부는 실리고 초과분만 잘린다
    assert [f.name for f in embed.fields] == [f"n{i}" for i in range(len(embed.fields))]


def test_build_card_embed_masks_every_slot():
    """author·title·description·field name·field value·footer — 한 슬롯이라도 새면 결함."""
    embed = discord_adapter.build_card_embed(
        {
            "author": "🧩 SECRET축",
            "title": "SECRET/repo",
            "description": "**차용** · SECRET 쓴다",
            "fields": [("👍 SECRET", "값 SECRET 있음", True)],
            "footer": "검토 SECRET · 기각 1",
            "color": 0x3ECF85,
        },
        ["SECRET"],
    )
    dumped = str(embed.to_dict())
    assert "SECRET" not in dumped
    assert dumped.count("***") == 6  # 6개 슬롯 전부 마스킹됨


def test_build_card_embed_ignores_unknown_keys_and_bad_color():
    """코어가 키를 더 넣거나 color 를 안 줘도 400 나지 않는다(규약: 없는 키 = 빈 슬롯)."""
    embed = discord_adapter.build_card_embed({"foo": "bar", "color": 0}, [])
    assert embed.color.value == discord_adapter._COLOR_INFO  # color=0 → 폴백
    assert embed.title is None and embed.description is None and embed.fields == []
