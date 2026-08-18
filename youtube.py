#!/usr/bin/env python3
"""youtube.py — 유튜브 재생목록 영상 추가·제거(OAuth refresh→access→list/insert/delete).

stdlib urllib 전용. 코어(bridge.py)가 'ㅁ추가'·'ㅁ삭제' 처리에서 호출한다. 검색(yt-dlp)은 어댑터
소관이고, 이 모듈은 순수 HTTP(YouTube Data API v3) — 코어 stdlib 원칙을 지킨다. 크리덴셜은
프로젝트 루트의 `.oauth_client.json`/`.oauth_token.json`(gitignore·커밋 금지)에서 **읽기만** 하며
값(토큰·시크릿)은 어디에도 로깅·노출하지 않는다. insert 전에 playlistItems.list 로 중복을 확인해
스킵하고, delete 는 2건 이상 매칭이면 지우지 않는다(remove_video — 오삭제 방지).
"""

from __future__ import annotations

import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from adapter import _NOREDIRECT_OPENER, fold_title

_ROOT = Path(__file__).resolve().parent
CLIENT_FILE = _ROOT / ".oauth_client.json"  # {"installed":{"client_id","client_secret",...}}
TOKEN_FILE = _ROOT / ".oauth_token.json"  # {"refresh_token": "..."}
# 대상 재생목록 "코딩"(개발자 소유·검증됨). 기본값일 뿐 — bridge.py 가 기동 시 .env 의
# MUSIC_PLAYLIST_ID 로 덮어쓴다(재생·추가가 같은 목록을 보게 하는 단일 출처).
PLAYLIST_ID = "PLfYAqOSmXQFQ"
_API = "https://www.googleapis.com/youtube/v3/playlistItems"
_TOKEN_URI = "https://oauth2.googleapis.com/token"  # client 파일에 없을 때 폴백
_UA = "claude_bridge_youtube/1.0"
_TIMEOUT = 15

# access token 메모리 캐시(만료 전 재사용). ponytail: 단일 워커가 직렬 처리라 잠금 불필요 —
# 멀티스레드에서 갱신이 필요해지면 lock 을 추가한다.
_access_token = ""
_access_exp = 0.0


def _http_json(req: urllib.request.Request) -> dict[str, Any]:
    """고정 host(googleapis.com) GET/POST → JSON. **리다이렉트 미추종**(adapter 공용 opener).

    urllib 의 기본 리다이렉트는 `Content-*` 헤더만 떨구고 **`Authorization: Bearer` 는 재전송**한다
    → 3xx 를 따라가면 액세스 토큰이 그 목적지로 간다. host 가 하드코딩이라 실현성은 낮지만
    비용이 한 줄이라 막아둔다(bridge.fetch_rest_probe·us_digest._get 과 같은 opener).
    """
    with _NOREDIRECT_OPENER.open(req, timeout=_TIMEOUT) as resp:
        data: dict[str, Any] = json.loads(resp.read().decode("utf-8"))
    return data


def _refresh_access() -> tuple[str, int]:
    """refresh_token → (access_token, expires_in). 크리덴셜 파일에서 읽어 token_uri 로 교환."""
    client = json.loads(CLIENT_FILE.read_text(encoding="utf-8"))["installed"]
    token = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    body = urllib.parse.urlencode(
        {
            "client_id": client["client_id"],
            "client_secret": client["client_secret"],
            "refresh_token": token["refresh_token"],
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        client.get("token_uri", _TOKEN_URI),
        data=body,
        headers={"User-Agent": _UA, "Content-Type": "application/x-www-form-urlencoded"},
    )
    payload = _http_json(req)
    return str(payload["access_token"]), int(payload.get("expires_in", 3600))


def _get_access() -> str:
    """유효한 access token(만료 60s 여유 전이면 캐시 재사용, 아니면 refresh)."""
    global _access_token, _access_exp
    if _access_token and time.time() < _access_exp - 60:
        return _access_token
    _access_token, expires_in = _refresh_access()
    _access_exp = time.time() + expires_in
    return _access_token


def _list_items(access: str) -> list[tuple[str, str, str]]:
    """대상 재생목록의 (playlistItem id, videoId, 제목) 목록. 50개씩 pageToken 순회.

    ⚠️ **playlistItems.delete 는 videoId 가 아니라 playlistItem id 를 받는다** — 삭제까지 쓰려면
    두 id 를 함께 들고 있어야 해서 {videoId: 제목} 대신 3튜플로 돌려준다(추가의 중복확인은
    add_video 가 여기서 {videoId: 제목} 을 만들어 쓴다 — 순회는 하나로 공유).
    """
    out: list[tuple[str, str, str]] = []
    page = ""
    while True:
        params = {"part": "snippet", "playlistId": PLAYLIST_ID, "maxResults": "50"}
        if page:
            params["pageToken"] = page
        req = urllib.request.Request(
            f"{_API}?{urllib.parse.urlencode(params)}",
            headers={"User-Agent": _UA, "Authorization": f"Bearer {access}"},
        )
        payload = _http_json(req)
        for item in payload.get("items", []):
            snip = item.get("snippet", {})
            vid = snip.get("resourceId", {}).get("videoId")
            item_id = item.get("id")
            if vid and item_id:
                out.append((str(item_id), str(vid), str(snip.get("title", vid))))
        page = payload.get("nextPageToken", "")
        if not page:
            return out


def _insert(access: str, video_id: str) -> str:
    """playlistItems.insert → 추가된 영상 제목."""
    body = json.dumps(
        {
            "snippet": {
                "playlistId": PLAYLIST_ID,
                "resourceId": {"kind": "youtube#video", "videoId": video_id},
            }
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{_API}?part=snippet",
        data=body,
        headers={
            "User-Agent": _UA,
            "Authorization": f"Bearer {access}",
            "Content-Type": "application/json",
        },
    )
    payload = _http_json(req)
    snip: dict[str, Any] = payload.get("snippet", {})
    return str(snip.get("title", video_id))


def _delete(access: str, item_id: str) -> None:
    """playlistItems.delete(파괴적) — 성공은 **204 무본문**이라 _http_json 을 쓰면 안 된다.

    JSON 파싱이 빈 본문에서 터져 성공을 실패로 오판한다. 그래서 여기만 opener 를 직접 열어
    상태코드만 본다(리다이렉트 미추종은 _http_json 과 동일 — Bearer 재전송 차단).
    """
    req = urllib.request.Request(
        f"{_API}?{urllib.parse.urlencode({'id': item_id})}",
        headers={"User-Agent": _UA, "Authorization": f"Bearer {access}"},
        method="DELETE",
    )
    with _NOREDIRECT_OPENER.open(req, timeout=_TIMEOUT) as resp:  # 비2xx 는 HTTPError 로 올라온다
        if resp.status not in (200, 204):
            raise ValueError(f"삭제 거부(HTTP {resp.status})")


def _reason(e: Exception) -> str:
    """실패 사유(비밀값 노출 없이) — HTTPError 는 status, 그 외는 예외 타입만.

    ⚠️ `FileNotFoundError` 만 따로 문구를 준다 — `.oauth_client.json`·`.oauth_token.json` 은
    **gitignore 라 `git pull` 로 안 따라온다**(2026-08-08 노트북에서 실제로 발생). 그런데 이 예외는
    `OSError` 라 `add_video` 의 except 에 삼켜져 **`오류(FileNotFoundError)`** 로만 회신됐고,
    폰에서 그 문구로는 "네트워크 문제인가 API 문제인가"를 가릴 수 없었다.
    파일명은 적지 않는다 — 이 회신은 **비인가 서버 멤버도 보는 채널**로 나간다(`_playlist_bypass`).
    """
    if isinstance(e, urllib.error.HTTPError):
        return f"YouTube API 오류(HTTP {e.code})"
    if isinstance(e, FileNotFoundError):
        return "OAuth 자격증명 없음 — 이 PC 에 인증 파일이 설정되지 않았습니다"
    return f"오류({type(e).__name__})"


def add_video(video_id: str) -> tuple[str, str]:
    """videoId 를 재생목록에 추가. 반환 (status, detail):

    - ("added", 제목): 새로 추가됨
    - ("dup", 제목): 이미 있어 스킵(insert 안 함)
    - ("fail", 사유): 인증·네트워크·API 오류(비밀값 미포함)

    중복은 insert 전에 playlistItems.list 로 확인한다. 예외는 삼켜 회신용 사유로만 변환한다.
    """
    try:
        access = _get_access()
        existing = {vid: title for _item, vid, title in _list_items(access)}
    # URLError/HTTPError ⊂ OSError, json ⊂ ValueError, 응답 잘림(IncompleteRead 등) = HTTPException
    # (OSError 아님 — fetch_rest_probe 선례처럼 명시 포집해야 회신 유실 없이 사유로 변환).
    except (OSError, ValueError, KeyError, http.client.HTTPException) as e:
        return ("fail", _reason(e))
    if video_id in existing:
        return ("dup", existing[video_id])
    try:
        return ("added", _insert(access, video_id))
    except (OSError, ValueError, KeyError, http.client.HTTPException) as e:
        return ("fail", _reason(e))


def list_titles() -> tuple[str, list[tuple[str, str]]]:
    """재생목록 전곡 → ("", [(videoId, 제목), …]). 실패는 (사유, []) — 'ㅁ목록' 회신용.

    사설 `_list_items` 를 코어가 직접 부르지 않도록 두는 공개 표면(3튜플의 playlistItem id 는
    삭제 전용이라 뺀다). 예외 포집은 add_video·remove_video 와 같은 집합 — 비밀값 미포함 사유로만
    변환한다(이 회신은 비인가 서버 멤버도 보는 채널로 나간다).
    """
    try:
        return ("", [(vid, title) for _item, vid, title in _list_items(_get_access())])
    except (OSError, ValueError, KeyError, http.client.HTTPException) as e:
        return (_reason(e), [])


def remove_video(query: str) -> tuple[str, str, str]:
    """제목으로 재생목록에서 영상 1건 제거. 반환 (status, detail, videoId):

    - ("removed", 제목, videoId): 1건만 매칭 → 삭제 완료
    - ("none", query, ""): 매칭 0건
    - ("many", "제목1 / 제목2 …", ""): **2건 이상이면 지우지 않고** 후보를 돌려준다(파괴적 경로라
      오삭제 방지 — 사용자가 더 정확히 적어 다시 부른다). 후보는 최대 5개·제목 40자로 자른다.
    - ("fail", 사유, ""): 인증·네트워크·API 오류(비밀값 미포함 — 이 회신은 비인가 서버 멤버도 본다)

    매칭 = 공백접기+casefold(fold_title) **정확일치 우선, 없을 때만 부분 포함**.
    이것이 고치는 것은 «제목 A가 제목 B의 부분문자열이면(곡 ⊂ 곡 (Remix)) A를 정확히 쳐도 늘
    many 라 영영 못 지운다» 쪽이다.
    ⚠️ **오삭제를 없애지는 못한다** — 정확일치가 0건이면 그대로 부분일치로 떨어지므로,
    'ㅁ삭제 좋은날' 인데 목록에 "좋은날 리믹스" 하나뿐이면 그 곡이 확인 없이 지워진다.
    **감수한 트레이드오프**다: 부분일치 단독도 many 로 막으면 긴 제목을 통째로 정확히 쳐야만
    삭제돼 명령이 사실상 못 쓰게 된다. 회신이 지운 제목을 그대로 알려주고 'ㅁ추가' 로 되돌릴 수
    있다는 것이 이 선택의 근거다. 예외 포집은 add_video 와 동일 집합.
    """
    key = fold_title(query)
    if (
        not key
    ):  # 빈 키는 모든 제목에 포함돼 곡이 1개뿐인 목록의 그 곡을 지운다(파괴적 함수 자체 가드)
        return ("none", query, "")
    try:
        items = _list_items(_get_access())
    except (OSError, ValueError, KeyError, http.client.HTTPException) as e:
        return ("fail", _reason(e), "")
    exact = [it for it in items if fold_title(it[2]) == key]
    hits = exact or [it for it in items if key in fold_title(it[2])]
    if not hits:
        return ("none", query, "")
    if len(hits) > 1:
        return ("many", " / ".join(title[:40] for _item, _vid, title in hits[:5]), "")
    item_id, video_id, title = hits[0]
    try:
        _delete(_get_access(), item_id)
    except (OSError, ValueError, KeyError, http.client.HTTPException) as e:
        return ("fail", _reason(e), "")
    return ("removed", title, video_id)
