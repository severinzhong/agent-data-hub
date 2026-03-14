from __future__ import annotations

from pathlib import Path
import os
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from core.base import BaseSource
from core.models import ContentRecord, build_content_ref
from core.registry import build_default_registry
from sources.ashare.source import AShareSource
from sources.xiaohongshu.normalize import build_note_opaque_id
from store.db import Store
from tests.fixtures import default_storage_specs


class UpdateFlowTests(unittest.TestCase):
    def test_ashare_uses_shared_base_update_flow(self) -> None:
        self.assertIs(AShareSource.update, BaseSource.update)

    def test_cryptocompare_uses_shared_base_update_flow(self) -> None:
        registry = build_default_registry(store=None)
        self.assertIn("cryptocompare", registry.list_names())
        self.assertIs(registry.build("cryptocompare").__class__.update, BaseSource.update)

    def test_xiaohongshu_uses_shared_base_update_flow(self) -> None:
        registry = build_default_registry(store=None)
        self.assertIn("xiaohongshu", registry.list_names())
        self.assertIs(registry.build("xiaohongshu").__class__.update, BaseSource.update)

    def test_update_requires_subscription_and_deduplicates(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        source = AShareSource(store=store)
        kline_payload = {"rc": 0, "data": {"name": "贵州茅台", "klines": ["2026-03-11,1402.99,1399.94,1405.99,1398.02,120,1200000.00,0,0,0,0"]}}
        with self.assertRaisesRegex(Exception, "not subscribed"):
            source.update("sh600519", limit=1)
        source.subscribe("sh600519")
        with patch.object(source.http, "get_json", return_value=kline_payload):
            first = source.update("sh600519", limit=1)
            second = source.update("sh600519", limit=1)
        self.assertEqual(first.saved_count, 1)
        self.assertEqual(second.skipped_count, 1)

    def test_xiaohongshu_update_stops_after_seen_note(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        store.set_source_config("xiaohongshu", "cookie", "a1=token; web_session=session", "string", True)
        source = build_default_registry(store=store).build("xiaohongshu")
        source.subscribe("user/user-1")
        store.upsert_content(
            ContentRecord(
                source="xiaohongshu",
                channel_key="user/user-1",
                record_type="note",
                external_id="note-3",
                title="已存在",
                url="https://www.xiaohongshu.com/explore/note-3",
                snippet="已存在",
                author="作者A",
                published_at="2026-03-10T00:00:00+00:00",
                fetched_at="2026-03-13T00:00:00+00:00",
                raw_payload="{}",
                dedup_key="note:note-3",
                content_ref=build_content_ref("xiaohongshu", build_note_opaque_id("note-3")),
            )
        )

        class FakeClient:
            page_calls = 0

            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def get_user_notes(self, user_id: str, *, cursor: str = "") -> dict:
                _ = user_id
                self.__class__.page_calls += 1
                if cursor:
                    return {"notes": [], "has_more": False, "cursor": ""}
                return {
                    "notes": [
                        {
                            "note_id": "note-1",
                            "desc": "第一篇\n正文",
                            "user": {"user_id": "user-1", "nickname": "作者A"},
                            "time": 1773446400000,
                        },
                        {
                            "note_id": "note-2",
                            "desc": "第二篇\n正文",
                            "user": {"user_id": "user-1", "nickname": "作者A"},
                            "time": 1773360000000,
                        },
                        {
                            "note_id": "note-3",
                            "desc": "已存在\n正文",
                            "user": {"user_id": "user-1", "nickname": "作者A"},
                            "time": 1773273600000,
                        },
                    ],
                    "has_more": True,
                    "cursor": "cursor-2",
                }

        with patch("sources.xiaohongshu.source.XiaohongshuClient", FakeClient):
            summary = source.update("user/user-1", limit=10)

        self.assertEqual(summary.saved_count, 2)
        self.assertEqual(FakeClient.page_calls, 1)

    def test_xiaohongshu_update_respects_since_before_fetching_older_pages(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        store.set_source_config("xiaohongshu", "cookie", "a1=token; web_session=session", "string", True)
        source = build_default_registry(store=store).build("xiaohongshu")
        source.subscribe("user/user-1")
        since = datetime(2026, 3, 11, tzinfo=UTC)

        class FakeClient:
            page_calls = 0

            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def get_user_notes(self, user_id: str, *, cursor: str = "") -> dict:
                _ = user_id
                self.__class__.page_calls += 1
                if cursor == "":
                    return {
                        "notes": [
                            {
                                "note_id": "note-1",
                                "desc": "第一篇\n正文",
                                "user": {"user_id": "user-1", "nickname": "作者A"},
                                "time": 1773446400000,
                            },
                            {
                                "note_id": "note-2",
                                "desc": "第二篇\n正文",
                                "user": {"user_id": "user-1", "nickname": "作者A"},
                                "time": 1773360000000,
                            },
                        ],
                        "has_more": True,
                        "cursor": "cursor-2",
                    }
                return {
                    "notes": [
                        {
                            "note_id": "note-3",
                            "desc": "第三篇\n正文",
                            "user": {"user_id": "user-1", "nickname": "作者A"},
                            "time": 1773187200000,
                        }
                    ],
                    "has_more": False,
                    "cursor": "",
                }

        with patch("sources.xiaohongshu.source.XiaohongshuClient", FakeClient):
            summary = source.update("user/user-1", since=since, limit=10)

        self.assertEqual(summary.saved_count, 2)
        self.assertEqual(FakeClient.page_calls, 2)

    def test_xiaohongshu_update_enriches_missing_time_from_note_detail(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        store.set_source_config("xiaohongshu", "cookie", "a1=token; web_session=session", "string", True)
        source = build_default_registry(store=store).build("xiaohongshu")
        source.subscribe("user/user-1")

        class FakeClient:
            detail_calls: list[tuple[str, str]] = []

            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def get_user_notes(self, user_id: str, *, cursor: str = "") -> dict:
                _ = user_id, cursor
                return {
                    "notes": [
                        {
                            "note_id": "note-1",
                            "xsec_token": "token-1",
                            "display_title": "第一篇",
                            "user": {"user_id": "user-1", "nickname": "作者A"},
                        }
                    ],
                    "has_more": False,
                    "cursor": "",
                }

            def get_note_detail(self, note_id: str, *, xsec_token: str = "", xsec_source: str = "pc_feed") -> dict:
                self.__class__.detail_calls.append((note_id, xsec_token))
                _ = xsec_source
                return {
                    "items": [
                        {
                            "id": note_id,
                            "note_card": {
                                "note_id": note_id,
                                "display_title": "第一篇",
                                "time": 1773446400000,
                                "user": {"user_id": "user-1", "nickname": "作者A"},
                            },
                        }
                    ]
                }

        with patch("sources.xiaohongshu.source.XiaohongshuClient", FakeClient):
            summary = source.update("user/user-1", limit=10)

        self.assertEqual(summary.saved_count, 1)
        self.assertEqual(FakeClient.detail_calls, [("note-1", "token-1")])


if __name__ == "__main__":
    unittest.main()
