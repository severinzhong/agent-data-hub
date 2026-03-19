from __future__ import annotations

import json
import random
import time
from typing import Any

from fetchers.base import FetchResponse, RiskMarkers
from fetchers.http import HttpFetcher

from core.protocol import AuthRequiredError, RemoteExecutionError

from .constants import CREATOR_HOST, EDITH_HOST, HOME_URL, USER_AGENT
from .signing import build_get_uri, sign_creator, sign_main_api


DIRECT_PROXY_VALUE = "direct"


class XiaohongshuClient:
    def __init__(
        self,
        *,
        cookies: dict[str, str],
        timeout: float = 30.0,
        request_delay: float = 0.5,
        max_retries: int = 3,
        proxy_url: str | None = None,
        fetcher: HttpFetcher | None = None,
    ) -> None:
        self.cookies = dict(cookies)
        self._http = fetcher or HttpFetcher(proxy_url=proxy_url)
        self._timeout = timeout
        self._request_delay = request_delay
        self._max_retries = max_retries

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> XiaohongshuClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def get_self_info(self) -> dict[str, Any]:
        data = self._main_api_get("/api/sns/web/v2/user/me")
        if not isinstance(data, dict):
            raise RemoteExecutionError("xiaohongshu returned invalid self info payload")
        return data

    def search_users(self, keyword: str) -> dict[str, Any]:
        data = self._creator_post(
            "/web_api/sns/v1/search/user_info",
            {
                "keyword": keyword,
                "search_id": str(int(time.time() * 1000)),
                "page": {"page_size": 20, "page": 1},
            },
        )
        if not isinstance(data, dict):
            raise RemoteExecutionError("xiaohongshu returned invalid user search payload")
        return data

    def search_notes(
        self,
        keyword: str,
        *,
        page: int = 1,
        page_size: int = 20,
        sort: str = "general",
        note_type: int = 0,
    ) -> dict[str, Any]:
        data = self._main_api_post(
            "/api/sns/web/v1/search/notes",
            {
                "keyword": keyword,
                "page": page,
                "page_size": page_size,
                "search_id": self._search_id(),
                "sort": sort,
                "note_type": note_type,
                "ext_flags": [],
                "filters": [],
                "geo": "",
                "image_formats": ["jpg", "webp", "avif"],
            },
        )
        if not isinstance(data, dict):
            raise RemoteExecutionError("xiaohongshu returned invalid note search payload")
        return data

    def get_user_notes(self, user_id: str, *, cursor: str = "") -> dict[str, Any]:
        data = self._main_api_get(
            "/api/sns/web/v1/user_posted",
            {
                "num": 30,
                "cursor": cursor,
                "user_id": user_id,
                "image_scenes": "FD_WM_WEBP",
            },
        )
        if not isinstance(data, dict):
            raise RemoteExecutionError("xiaohongshu returned invalid user notes payload")
        return data

    def get_note_detail(self, note_id: str, *, xsec_token: str = "", xsec_source: str = "pc_feed") -> dict[str, Any]:
        data = self._main_api_post(
            "/api/sns/web/v1/feed",
            {
                "source_note_id": note_id,
                "image_formats": ["jpg", "webp", "avif"],
                "extra": {"need_body_topic": "1"},
                "xsec_source": xsec_source,
                "xsec_token": xsec_token,
            },
        )
        if not isinstance(data, dict):
            raise RemoteExecutionError("xiaohongshu returned invalid note detail payload")
        return data

    def like_note(self, note_id: str) -> dict[str, Any]:
        return self._expect_dict(self._main_api_post("/api/sns/web/v1/note/like", {"note_oid": note_id}))

    def unlike_note(self, note_id: str) -> dict[str, Any]:
        return self._expect_dict(self._main_api_post("/api/sns/web/v1/note/dislike", {"note_oid": note_id}))

    def favorite_note(self, note_id: str) -> dict[str, Any]:
        return self._expect_dict(self._main_api_post("/api/sns/web/v1/note/collect", {"note_id": note_id}))

    def unfavorite_note(self, note_id: str) -> dict[str, Any]:
        return self._expect_dict(self._main_api_post("/api/sns/web/v1/note/uncollect", {"note_ids": note_id}))

    def post_comment(self, note_id: str, content: str) -> dict[str, Any]:
        return self._expect_dict(
            self._main_api_post(
                "/api/sns/web/v1/comment/post",
                {
                    "note_id": note_id,
                    "content": content,
                    "at_users": [],
                },
            )
        )

    def _expect_dict(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise RemoteExecutionError("xiaohongshu returned invalid payload")
        return value

    def _main_api_get(self, uri: str, params: dict[str, str | int | list[str]] | None = None) -> Any:
        headers = {
            **self._base_headers(),
            **sign_main_api("GET", uri, self.cookies, params=params),
        }
        url = f"{EDITH_HOST}{build_get_uri(uri, params)}"
        response = self._request_with_retry("GET", url, headers=headers)
        return self._handle_response(response)

    def _main_api_post(self, uri: str, data: dict[str, Any]) -> Any:
        headers = {
            **self._base_headers(),
            **sign_main_api("POST", uri, self.cookies, payload=data),
        }
        response = self._request_with_retry(
            "POST",
            f"{EDITH_HOST}{uri}",
            headers=headers,
            content=json.dumps(data, separators=(",", ":")),
        )
        return self._handle_response(response)

    def _creator_post(self, uri: str, data: dict[str, Any]) -> Any:
        host = self._creator_host(uri)
        headers = {
            **self._base_headers(),
            **sign_creator(f"url={uri}", data, self.cookies["a1"]),
            "origin": CREATOR_HOST,
            "referer": f"{CREATOR_HOST}/",
        }
        response = self._request_with_retry(
            "POST",
            f"{host}{uri}",
            headers=headers,
            content=json.dumps(data, separators=(",", ":")),
        )
        return self._handle_response(response)

    def _creator_host(self, uri: str) -> str:
        if uri.startswith("/api/galaxy/"):
            return CREATOR_HOST
        return EDITH_HOST

    def _base_headers(self) -> dict[str, str]:
        return {
            "user-agent": USER_AGENT,
            "content-type": "application/json;charset=UTF-8",
            "cookie": self._cookie_header(),
            "origin": HOME_URL,
            "referer": f"{HOME_URL}/",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "dnt": "1",
            "priority": "u=1, i",
        }

    def _cookie_header(self) -> str:
        return "; ".join(f"{key}={value}" for key, value in self.cookies.items())

    def _request_with_retry(self, method: str, url: str, **kwargs) -> FetchResponse:
        response = self._http.request(
            method,
            url,
            policy=self._request_policy_name(method, url),
            risk_markers=RiskMarkers(
                status_codes=(461, 471),
                header_keys=("verifytype", "verifyuuid"),
            ),
            **kwargs,
        )
        if response.cookies:
            self.cookies.update(response.cookies)
        return response

    def _handle_response(self, response: FetchResponse) -> Any:
        if response.risk_signal is not None:
            raise RemoteExecutionError(
                f"xiaohongshu requires verification: type={response.headers.get('verifytype', 'unknown')}, "
                f"uuid={response.headers.get('verifyuuid', 'unknown')}"
            )

        text = response.text()
        if not text:
            return None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RemoteExecutionError(f"xiaohongshu returned non-JSON response: {text[:200]}") from exc

        if payload.get("success"):
            return payload.get("data", payload.get("success"))

        code = payload.get("code")
        if code == -100:
            raise AuthRequiredError("xiaohongshu cookie is invalid or expired; please update xiaohongshu.cookie")
        if code == 300015:
            raise RemoteExecutionError("xiaohongshu signature verification failed")
        if code == 300012:
            raise RemoteExecutionError("xiaohongshu IP was blocked")
        raise RemoteExecutionError(f"xiaohongshu API error: {json.dumps(payload, ensure_ascii=False)[:300]}")

    def _search_id(self) -> str:
        return f"{int(time.time() * 1000)}{random.randint(1000, 9999)}"

    def _request_policy_name(self, method: str, url: str) -> dict[str, object]:
        if method == "GET" and "search" in url:
            base = "search"
        elif "note/" in url or "comment/" in url:
            base = "interact"
        else:
            base = "update"
        return {
            "base": base,
            "timeout_s": self._timeout,
            "min_interval_ms": int(self._request_delay * 1000),
            "jitter_ms": 500,
            "max_retries": self._max_retries,
            "backoff_ms": 1000,
            "retry_statuses": (429, 500, 502, 503, 504),
        }
