#!/usr/bin/env python3
"""youtube.py — 유튜브 재생목록에 영상 추가(OAuth refresh→access→list/insert). stdlib urllib 전용.

코어(bridge.py)가 'ㅁ추가' 처리에서 호출한다. 검색(yt-dlp)은 어댑터 소관이고, 이 모듈은 순수
HTTP(YouTube Data API v3) — 코어 stdlib 원칙을 지킨다. 크리덴셜은 프로젝트 루트의
`.oauth_client.json`/`.oauth_token.json`(gitignore·커밋 금지)에서 **읽기만** 하며 값(토큰·시크릿)은
어디에도 로깅·노출하지 않는다. insert 전에 playlistItems.list 로 중복을 확인해 스킵한다.
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

from adapter import _NOREDIRECT_OPENER

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


def _list_video_ids(access: str) -> dict[str, str]:
    """대상 재생목록의 {videoId: 제목}. 50개씩 pageToken 순회."""
    out: dict[str, str] = {}
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
            if vid:
                out[vid] = snip.get("title", vid)
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


def _reason(e: Exception) -> str:
    """실패 사유(비밀값 노출 없이) — HTTPError 는 status, 그 외는 예외 타입만."""
    if isinstance(e, urllib.error.HTTPError):
        return f"YouTube API 오류(HTTP {e.code})"
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
        existing = _list_video_ids(access)
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
