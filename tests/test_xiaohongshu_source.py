from __future__ import annotations

from pathlib import Path
import os
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

import json
import unittest
from unittest.mock import patch

from core.protocol import AuthRequiredError, RemoteExecutionError
from sources.xiaohongshu.client import XiaohongshuClient
from sources.xiaohongshu.cookies import parse_cookie_header
from sources.xiaohongshu.normalize import (
    build_note_opaque_id,
    parse_note_opaque_id,
    parse_user_channel_key,
    summarize_note_text,
)


class XiaohongshuSourceHelpersTests(unittest.TestCase):
    def test_parse_cookie_header_splits_full_header_string(self) -> None:
        cookies = parse_cookie_header("a1=token; web_session=session; webId=wid")

        self.assertEqual(
            cookies,
            {
                "a1": "token",
                "web_session": "session",
                "webId": "wid",
            },
        )

    def test_parse_cookie_header_rejects_missing_a1(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "a1"):
            parse_cookie_header("web_session=session; webId=wid")

    def test_parse_cookie_header_rejects_empty_cookie_pair(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "cookie"):
            parse_cookie_header("a1=token; broken")

    def test_parse_user_channel_key_accepts_user_prefix(self) -> None:
        self.assertEqual(parse_user_channel_key("user/5f7c8abc"), "5f7c8abc")

    def test_parse_user_channel_key_rejects_invalid_prefix(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "user/<user_id>"):
            parse_user_channel_key("topic/5f7c8abc")

    def test_note_opaque_id_round_trips(self) -> None:
        opaque_id = build_note_opaque_id("67abcd1234")

        self.assertEqual(opaque_id, "note:67abcd1234")
        self.assertEqual(parse_note_opaque_id(opaque_id), "67abcd1234")

    def test_parse_note_opaque_id_rejects_non_note_prefix(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "note:<note_id>"):
            parse_note_opaque_id("comment:123")

    def test_summarize_note_text_uses_first_non_empty_line_as_title(self) -> None:
        title, snippet = summarize_note_text("\n\n第一行标题\n第二行正文\n第三行补充")

        self.assertEqual(title, "第一行标题")
        self.assertIn("第二行正文", snippet)

    def test_summarize_note_text_builds_longer_snippet(self) -> None:
        title, snippet = summarize_note_text("第一行标题\n第二行正文\n第三行补充")

        self.assertLess(len(title), len(snippet))
        self.assertEqual(title, "第一行标题")
        self.assertEqual(snippet, "第一行标题 第二行正文 第三行补充")


if __name__ == "__main__":
    unittest.main()


class FakeCookies:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self._values = values or {}

    def items(self):
        return self._values.items()


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        payload: dict | None = None,
        text: str | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.cookies = FakeCookies(cookies)
        if text is not None:
            self.text = text
        elif payload is not None:
            self.text = json.dumps(payload)
        else:
            self.text = ""


class RecordingHttpClient:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.requests: list[dict[str, object]] = []

    def request(self, method: str, url: str, **kwargs):
        self.requests.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)

    def close(self) -> None:
        return None


class XiaohongshuClientTests(unittest.TestCase):
    def _build_client(self, responses: list[FakeResponse]) -> tuple[XiaohongshuClient, RecordingHttpClient]:
        fake_http = RecordingHttpClient(responses)
        with patch("sources.xiaohongshu.client.httpx.Client", return_value=fake_http):
            client = XiaohongshuClient(cookies={"a1": "token", "web_session": "session"}, request_delay=0)
        return client, fake_http

    def test_client_uses_environment_proxy_when_unset(self) -> None:
        with patch("sources.xiaohongshu.client.httpx.Client", return_value=RecordingHttpClient([])) as client_class:
            XiaohongshuClient(cookies={"a1": "token"}, request_delay=0)

        self.assertTrue(client_class.call_args.kwargs["trust_env"])
        self.assertNotIn("proxy", client_class.call_args.kwargs)

    def test_client_direct_proxy_override_disables_environment_proxy(self) -> None:
        with patch("sources.xiaohongshu.client.httpx.Client", return_value=RecordingHttpClient([])) as client_class:
            XiaohongshuClient(cookies={"a1": "token"}, request_delay=0, proxy_url="direct")

        self.assertFalse(client_class.call_args.kwargs["trust_env"])
        self.assertIsNone(client_class.call_args.kwargs["proxy"])

    def test_client_explicit_proxy_disables_environment_proxy_inheritance(self) -> None:
        with patch("sources.xiaohongshu.client.httpx.Client", return_value=RecordingHttpClient([])) as client_class:
            XiaohongshuClient(cookies={"a1": "token"}, request_delay=0, proxy_url="http://127.0.0.1:7890")

        self.assertFalse(client_class.call_args.kwargs["trust_env"])
        self.assertEqual(client_class.call_args.kwargs["proxy"], "http://127.0.0.1:7890")

    @patch("sources.xiaohongshu.client.sign_creator", return_value={"x-s": "creator-sign", "x-t": "123"})
    def test_search_users_posts_creator_endpoint(self, _sign_creator) -> None:
        client, fake_http = self._build_client([FakeResponse(payload={"success": True, "data": {"users": []}})])

        result = client.search_users("openai")

        self.assertEqual(result, {"users": []})
        request = fake_http.requests[0]
        self.assertEqual(request["method"], "POST")
        self.assertEqual(request["url"], "https://edith.xiaohongshu.com/web_api/sns/v1/search/user_info")
        payload = json.loads(str(request["content"]))
        self.assertEqual(payload["keyword"], "openai")
        self.assertEqual(payload["page"], {"page_size": 20, "page": 1})

    @patch("sources.xiaohongshu.client.sign_main_api", return_value={"x-s": "main-sign", "x-s-common": "common", "x-t": "1"})
    def test_search_notes_posts_search_notes_endpoint(self, _sign_main_api) -> None:
        client, fake_http = self._build_client([FakeResponse(payload={"success": True, "data": {"items": []}})])

        result = client.search_notes("openai", page=2)

        self.assertEqual(result, {"items": []})
        request = fake_http.requests[0]
        self.assertEqual(request["method"], "POST")
        self.assertEqual(request["url"], "https://edith.xiaohongshu.com/api/sns/web/v1/search/notes")
        payload = json.loads(str(request["content"]))
        self.assertEqual(payload["keyword"], "openai")
        self.assertEqual(payload["page"], 2)
        self.assertEqual(payload["page_size"], 20)
        self.assertIn("search_id", payload)

    @patch("sources.xiaohongshu.client.sign_main_api", return_value={"x-s": "main-sign", "x-s-common": "common", "x-t": "1"})
    def test_get_user_notes_uses_user_posted_get(self, _sign_main_api) -> None:
        client, fake_http = self._build_client([FakeResponse(payload={"success": True, "data": {"notes": []}})])

        result = client.get_user_notes("user-123", cursor="cursor-1")

        self.assertEqual(result, {"notes": []})
        request = fake_http.requests[0]
        self.assertEqual(request["method"], "GET")
        self.assertIn("/api/sns/web/v1/user_posted", str(request["url"]))
        self.assertIn("user_id=user-123", str(request["url"]))
        self.assertIn("cursor=cursor-1", str(request["url"]))

    @patch("sources.xiaohongshu.client.sign_main_api", return_value={"x-s": "main-sign", "x-s-common": "common", "x-t": "1"})
    def test_get_self_info_uses_profile_endpoint(self, _sign_main_api) -> None:
        client, fake_http = self._build_client([FakeResponse(payload={"success": True, "data": {"nickname": "demo"}})])

        result = client.get_self_info()

        self.assertEqual(result, {"nickname": "demo"})
        request = fake_http.requests[0]
        self.assertEqual(request["method"], "GET")
        self.assertEqual(request["url"], "https://edith.xiaohongshu.com/api/sns/web/v2/user/me")

    @patch("sources.xiaohongshu.client.sign_main_api", return_value={"x-s": "main-sign", "x-s-common": "common", "x-t": "1"})
    def test_like_and_favorite_actions_use_expected_payloads(self, _sign_main_api) -> None:
        client, fake_http = self._build_client(
            [
                FakeResponse(payload={"success": True, "data": {"ok": True}}),
                FakeResponse(payload={"success": True, "data": {"ok": True}}),
                FakeResponse(payload={"success": True, "data": {"ok": True}}),
                FakeResponse(payload={"success": True, "data": {"ok": True}}),
                FakeResponse(payload={"success": True, "data": {"ok": True}}),
            ]
        )

        client.like_note("note-1")
        client.unlike_note("note-1")
        client.favorite_note("note-1")
        client.unfavorite_note("note-1")
        client.post_comment("note-1", "hello")

        like_payload = json.loads(str(fake_http.requests[0]["content"]))
        unlike_payload = json.loads(str(fake_http.requests[1]["content"]))
        favorite_payload = json.loads(str(fake_http.requests[2]["content"]))
        unfavorite_payload = json.loads(str(fake_http.requests[3]["content"]))
        comment_payload = json.loads(str(fake_http.requests[4]["content"]))
        self.assertEqual(like_payload, {"note_oid": "note-1"})
        self.assertEqual(unlike_payload, {"note_oid": "note-1"})
        self.assertEqual(favorite_payload, {"note_id": "note-1"})
        self.assertEqual(unfavorite_payload, {"note_ids": "note-1"})
        self.assertEqual(comment_payload, {"note_id": "note-1", "content": "hello", "at_users": []})

    @patch("sources.xiaohongshu.client.sign_main_api", return_value={"x-s": "main-sign", "x-s-common": "common", "x-t": "1"})
    def test_client_maps_session_expired_to_auth_required(self, _sign_main_api) -> None:
        client, _fake_http = self._build_client([FakeResponse(payload={"success": False, "code": -100})])

        with self.assertRaises(AuthRequiredError):
            client.get_self_info()

    @patch("sources.xiaohongshu.client.sign_main_api", return_value={"x-s": "main-sign", "x-s-common": "common", "x-t": "1"})
    def test_client_keeps_signature_failures_as_remote_execution_error(self, _sign_main_api) -> None:
        client, _fake_http = self._build_client([FakeResponse(payload={"success": False, "code": 300015})])

        with self.assertRaises(RemoteExecutionError):
            client.get_self_info()

    @patch("sources.xiaohongshu.client.sign_main_api", return_value={"x-s": "main-sign", "x-s-common": "common", "x-t": "1"})
    def test_client_keeps_captcha_responses_as_remote_execution_error(self, _sign_main_api) -> None:
        client, _fake_http = self._build_client(
            [FakeResponse(status_code=461, headers={"verifytype": "captcha", "verifyuuid": "uuid-1"})]
        )

        with self.assertRaises(RemoteExecutionError):
            client.get_self_info()
