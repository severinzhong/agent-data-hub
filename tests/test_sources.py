from __future__ import annotations

from pathlib import Path
import os
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from unittest.mock import patch

from cli import main as cli_main
from core.config import ResolvedSourceConfig, SourceConfigEntry
from core.registry import build_default_registry
from sources.ashare.source import AShareSource
from sources.bbc.source import BbcSource, MANIFEST as BBC_MANIFEST, SOURCE_CLASS as BBC_SOURCE_CLASS
from sources.hackernews.source import HackerNewsSource
from sources.sina_finance_724.source import SinaFinance724Source
from sources.usstock.source import UsStockSource
from sources.wechatarticle.source import WechatArticleSource
from store.db import Store
from tests.fixtures import default_storage_specs


class SourceSmokeTests(unittest.TestCase):
    def _cryptocompare_coinlist_payload(self) -> dict:
        return {
            "Response": "Success",
            "Message": "Summary coin list succesfully returned!",
            "Data": {
                "BTC": {
                    "Id": "1182",
                    "Symbol": "BTC",
                    "FullName": "Bitcoin (BTC)",
                    "ImageUrl": "/media/37746251/btc.png",
                },
                "ETH": {
                    "Id": "7605",
                    "Symbol": "ETH",
                    "FullName": "Ethereum (ETH)",
                    "ImageUrl": "/media/37746238/eth.png",
                },
            },
        }

    def _cryptocompare_histoday_payload(self) -> dict:
        return {
            "Response": "Success",
            "Message": "",
            "Data": {
                "Aggregated": False,
                "TimeFrom": 1773100800,
                "TimeTo": 1773273600,
                "Data": [
                    {
                        "time": 1773100800,
                        "high": 71784.73,
                        "low": 68390.84,
                        "open": 68438.57,
                        "volumefrom": 41624.34,
                        "volumeto": 2928859556.81,
                        "close": 69961.25,
                        "conversionType": "direct",
                        "conversionSymbol": "",
                    },
                    {
                        "time": 1773187200,
                        "high": 71346.32,
                        "low": 68986.08,
                        "open": 69961.25,
                        "volumefrom": 34413.10,
                        "volumeto": 2418277293.19,
                        "close": 70205.19,
                        "conversionType": "direct",
                        "conversionSymbol": "",
                    },
                    {
                        "time": 1773273600,
                        "high": 70804.80,
                        "low": 69210.99,
                        "open": 70205.19,
                        "volumefrom": 24632.83,
                        "volumeto": 1725287811.50,
                        "close": 70450.43,
                        "conversionType": "direct",
                        "conversionSymbol": "",
                    },
                ],
            },
        }

    def _xiaohongshu_user_search_payload(self) -> dict:
        return {
            "users": [
                {
                    "user_id": "user-1",
                    "nickname": "OpenAI研究员",
                    "red_id": "openai_lab",
                    "images": "https://example.com/avatar.jpg",
                    "fans": "1200",
                }
            ]
        }

    def _xiaohongshu_note_search_payload(self) -> dict:
        return {
            "items": [
                {
                    "id": "note-1",
                    "note_card": {
                        "note_id": "note-1",
                        "desc": "第一行标题\n第二行正文",
                        "user": {
                            "user_id": "user-1",
                            "nickname": "作者A",
                        },
                        "time": 1773273600000,
                    },
                }
            ]
        }

    def _xiaohongshu_user_notes_payload(self) -> dict:
        return {
            "notes": [
                {
                    "note_id": "note-1",
                    "desc": "第一行标题\n第二行正文",
                    "user": {
                        "user_id": "user-1",
                        "nickname": "作者A",
                    },
                    "time": 1773273600000,
                },
                {
                    "note_id": "note-2",
                    "desc": "第二行标题\n更多正文",
                    "user": {
                        "user_id": "user-1",
                        "nickname": "作者A",
                    },
                    "time": 1773360000000,
                },
            ],
            "has_more": False,
            "cursor": "",
        }

    def test_bbc_source_declares_manifest_and_source_class(self) -> None:
        self.assertEqual(BBC_MANIFEST.identity.name, "bbc")
        self.assertEqual(BBC_MANIFEST.storage.table_name, "bbc_records")
        self.assertIs(BBC_SOURCE_CLASS, BbcSource)

    def test_bbc_content_search_and_update(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        source = BbcSource(store=store, config=ResolvedSourceConfig.empty("bbc"))
        source.subscribe("world")
        rss_body = """
        <rss><channel>
          <item><title>OpenAI 1</title><link>https://www.bbc.co.uk/1</link><description>s1</description><guid>bbc-1</guid><pubDate>Tue, 11 Mar 2026 09:00:00 GMT</pubDate></item>
          <item><title>OpenAI 2</title><link>https://www.bbc.co.uk/2</link><description>s2</description><guid>bbc-2</guid><pubDate>Tue, 10 Mar 2026 09:00:00 GMT</pubDate></item>
        </channel></rss>
        """
        with patch.object(source.http, "get_text", return_value=rss_body):
            summary = source.update("world", limit=2)
        self.assertEqual(summary.saved_count, 2)
        rows = store.list_content("bbc", "world", limit=10)
        self.assertEqual(len(rows), 2)
        search_html = """
        <div data-testid="default-promo"><a href="https://www.bbc.com/news/articles/search-1"><span aria-hidden="false">OpenAI search result</span></a><p class="Paragraph">search snippet</p></div>
        """
        with patch.object(source.http, "get_text", return_value=search_html):
            results = source.search_content(query="openai", limit=3)
        self.assertEqual(results[0].title, "OpenAI search result")

    def test_hackernews_content_search_and_update(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        source = HackerNewsSource(store=store)
        source.subscribe("top")
        channel_payload = {"hits": [{"objectID": "100", "title": "OpenAI infrastructure update", "url": "https://example.com/hn-1", "author": "bob", "created_at": "2026-03-11T09:00:00+00:00"}]}
        with patch.object(source.http, "get_json", return_value=channel_payload):
            summary = source.update("top", limit=1)
        self.assertEqual(summary.saved_count, 1)
        search_payload = {"hits": [{"objectID": "200", "title": "OpenAI search result", "url": "https://example.com/search", "author": "alice"}]}
        with patch.object(source.http, "get_json", return_value=search_payload):
            results = source.search_content(query="openai", limit=3)
        self.assertEqual(results[0].source, "hackernews")

    def test_ashare_channel_search_and_update(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        source = AShareSource(store=store)
        search_body = 'var suggestvalue="贵州茅台,11,600519,sh600519,贵州茅台,,贵州茅台,99,1,ESG,,;"'
        with patch.object(source.http, "get_text", return_value=search_body):
            channels = source.search_channels("贵州茅台", limit=3)
        self.assertEqual(channels[0].channel_key, "sh600519")
        source.subscribe("sh600519")
        kline_payload = {"rc": 0, "data": {"name": "贵州茅台", "klines": ["2026-03-11,1402.99,1399.94,1405.99,1398.02,120,1200000.00,0,0,0,0"]}}
        with patch.object(source.http, "get_json", return_value=kline_payload):
            summary = source.update("sh600519", limit=1)
        self.assertEqual(summary.saved_count, 1)

    def test_usstock_channel_search_and_update(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        source = UsStockSource(store=store)
        search_payload = {
            "QuotationCodeTable": {
                "Data": [
                    {
                        "Code": "SE",
                        "Name": "Sea Ltd ADR",
                        "JYS": "NYSE",
                        "Classify": "UsStock",
                        "MarketType": "106",
                        "SecurityTypeName": "美股",
                        "MktNum": "106",
                        "QuoteID": "106.SE",
                    }
                ],
                "Status": 0,
                "Message": "成功",
            }
        }
        with patch.object(source.http, "get_json", return_value=search_payload):
            channels = source.search_channels("sea", limit=3)
        self.assertEqual(channels[0].channel_key, "106.SE")
        source.subscribe("106.SE", display_name="Sea Ltd ADR")
        kline_payload = {
            "rc": 0,
            "data": {
                "code": "SE",
                "market": 106,
                "name": "Sea Ltd ADR",
                "klines": ["2026-03-12,86.395,85.510,89.000,85.240,1989810,172618012.000,4.28,-2.63,-2.310,0.34"],
            },
        }
        with patch.object(source.http, "get_json", return_value=kline_payload):
            summary = source.update("106.SE", limit=1)
        self.assertEqual(summary.saved_count, 1)

    def test_cryptocompare_channel_search_and_update(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        source = build_default_registry(store=store).build("cryptocompare")

        with patch.object(source.http, "get_json", return_value=self._cryptocompare_coinlist_payload()):
            channels = source.search_channels("btc", limit=3)
        self.assertEqual(channels[0].channel_key, "BTC")

        with patch.object(source.http, "get_json", return_value=self._cryptocompare_coinlist_payload()):
            source.subscribe("BTC")

        with patch.object(source.http, "get_json", return_value=self._cryptocompare_histoday_payload()):
            summary = source.update("BTC", limit=2)
        self.assertEqual(summary.saved_count, 2)

    def test_xiaohongshu_channel_search_and_update(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        store.set_source_config("xiaohongshu", "cookie", "a1=token; web_session=session", "string", True)
        source = build_default_registry(store=store).build("xiaohongshu")

        class FakeClient:
            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def search_users(self, keyword: str) -> dict:
                _ = keyword
                return SourceSmokeTests._xiaohongshu_user_search_payload(self=SourceSmokeTests())

            def get_user_notes(self, user_id: str, *, cursor: str = "") -> dict:
                _ = user_id, cursor
                return SourceSmokeTests._xiaohongshu_user_notes_payload(self=SourceSmokeTests())

        with patch("sources.xiaohongshu.source.XiaohongshuClient", FakeClient):
            channels = source.search_channels("openai", limit=3)
            self.assertEqual(channels[0].channel_key, "user/user-1")
            source.subscribe("user/user-1", display_name="OpenAI研究员")
            summary = source.update("user/user-1", limit=2)
        self.assertEqual(summary.saved_count, 2)

    def test_xiaohongshu_content_search_returns_note_content_ref(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        store.set_source_config("xiaohongshu", "cookie", "a1=token; web_session=session", "string", True)
        source = build_default_registry(store=store).build("xiaohongshu")

        class FakeClient:
            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def search_notes(self, keyword: str, *, page: int = 1, page_size: int = 20, sort: str = "general", note_type: int = 0) -> dict:
                _ = keyword, page, page_size, sort, note_type
                return SourceSmokeTests._xiaohongshu_note_search_payload(self=SourceSmokeTests())

        with patch("sources.xiaohongshu.source.XiaohongshuClient", FakeClient):
            results = source.search_content(query="openai", limit=5)

        self.assertEqual(results[0].content_ref, "xiaohongshu:content/note%3Anote-1")

    def test_xiaohongshu_content_search_by_channel_preview_does_not_write_store(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        store.set_source_config("xiaohongshu", "cookie", "a1=token; web_session=session", "string", True)
        source = build_default_registry(store=store).build("xiaohongshu")

        class FakeClient:
            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def get_user_notes(self, user_id: str, *, cursor: str = "") -> dict:
                _ = user_id, cursor
                return SourceSmokeTests._xiaohongshu_user_notes_payload(self=SourceSmokeTests())

        with patch("sources.xiaohongshu.source.XiaohongshuClient", FakeClient):
            results = source.search_content(channel_key="user/user-1", limit=5)

        self.assertEqual(len(results), 2)
        self.assertEqual(store.list_content("xiaohongshu", "user/user-1", limit=10), [])

    def test_sina_update_requires_subscription(self) -> None:
        store = Store(":memory:")
        store.init_schema(storage_specs=default_storage_specs())
        source = SinaFinance724Source(store=store)
        with self.assertRaisesRegex(Exception, "not subscribed"):
            source.update("0全部", limit=1)

    def test_wechatarticle_search_content(self) -> None:
        source = WechatArticleSource(store=None)
        html = """
        <li id="sogou_vr_11002601_box_0">
          <a id="sogou_vr_11002601_title_0" href="/link?url=test">OpenAI 深度文章</a>
          <p class="txt-info">search snippet</p>
          <span class="all-time-y2">OpenAI日报</span>
        </li>
        """
        with patch.object(source.http, "get_text", return_value=html):
            results = source.search_content(query="OpenAI", limit=2)
        self.assertEqual(results[0].title, "OpenAI 深度文章")

    def test_cli_content_search_jsonl_behaves_like_user_operation(self) -> None:
        html = """
        <li id="sogou_vr_11002601_box_0">
          <a id="sogou_vr_11002601_title_0" href="/link?url=test">OpenAI 深度文章</a>
          <p class="txt-info">search snippet</p>
          <span class="all-time-y2">OpenAI日报</span>
        </li>
        """
        output = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "wechat.db")),
            patch("sources.wechatarticle.source.WechatArticleSource._fetch_search_page_with_retry", return_value=html),
            redirect_stdout(output),
        ):
            self.assertEqual(cli_main.main(["content", "search", "--source", "wechatarticle", "--query", "OpenAI", "--jsonl", "--limit", "2"]), 0)
        rows = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["title"], "OpenAI 深度文章")

    def test_cli_usstock_flow_behaves_like_user_operation(self) -> None:
        search_payload = {
            "QuotationCodeTable": {
                "Data": [
                    {
                        "Code": "SE",
                        "Name": "Sea Ltd ADR",
                        "JYS": "NYSE",
                        "Classify": "UsStock",
                        "MarketType": "106",
                        "SecurityTypeName": "美股",
                        "MktNum": "106",
                        "QuoteID": "106.SE",
                    }
                ],
                "Status": 0,
                "Message": "成功",
            }
        }
        kline_payload = {
            "rc": 0,
            "data": {
                "code": "SE",
                "market": 106,
                "name": "Sea Ltd ADR",
                "klines": [
                    "2026-03-12,86.395,85.510,89.000,85.240,1989810,172618012.000,4.28,-2.63,-2.310,0.34",
                    "2026-03-11,88.410,87.820,89.950,85.950,4097618,358438160.000,4.49,-1.51,-1.350,0.69",
                ],
            },
        }

        def fake_get_json(url: str, **_kwargs):
            if "searchapi.eastmoney.com" in url:
                return search_payload
            if "push2his.eastmoney.com" in url:
                return kline_payload
            raise AssertionError(url)

        with tempfile.TemporaryDirectory() as temp_dir, patch.object(cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "usstock.db")):
            search_output = io.StringIO()
            with patch("fetchers.http.HttpFetcher.get_json", side_effect=fake_get_json), redirect_stdout(search_output):
                self.assertEqual(cli_main.main(["channel", "search", "--source", "usstock", "--query", "sea", "--limit", "2"]), 0)
            self.assertIn("106.SE", search_output.getvalue())

            sub_output = io.StringIO()
            with redirect_stdout(sub_output):
                self.assertEqual(
                    cli_main.main(["sub", "add", "--source", "usstock", "--channel", "106.SE", "--name", "Sea Ltd ADR"]),
                    0,
                )
            self.assertIn("Sea Ltd ADR", sub_output.getvalue())

            update_output = io.StringIO()
            with patch("fetchers.http.HttpFetcher.get_json", side_effect=fake_get_json), redirect_stdout(update_output):
                self.assertEqual(cli_main.main(["content", "update", "--source", "usstock", "--channel", "106.SE", "--limit", "2"]), 0)
            self.assertIn("saved_count", update_output.getvalue())

            query_output = io.StringIO()
            with redirect_stdout(query_output):
                self.assertEqual(cli_main.main(["content", "query", "--source", "usstock", "--channel", "106.SE", "--limit", "2"]), 0)
            self.assertIn("106.SE", query_output.getvalue())
            self.assertIn("85.510", query_output.getvalue())

    def test_cli_usstock_update_requires_subscription(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "usstock.db")):
            with self.assertRaisesRegex(Exception, "not subscribed"):
                cli_main.main(["content", "update", "--source", "usstock", "--channel", "106.SE", "--limit", "1"])

    def test_cli_cryptocompare_flow_behaves_like_user_operation(self) -> None:
        coinlist_payload = self._cryptocompare_coinlist_payload()
        histoday_payload = self._cryptocompare_histoday_payload()

        def fake_get_json(url: str, **_kwargs):
            if "all/coinlist" in url:
                return coinlist_payload
            if "v2/histoday" in url:
                return histoday_payload
            raise AssertionError(url)

        with tempfile.TemporaryDirectory() as temp_dir, patch.object(cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "cryptocompare.db")):
            search_output = io.StringIO()
            with patch("fetchers.http.HttpFetcher.get_json", side_effect=fake_get_json), redirect_stdout(search_output):
                self.assertEqual(cli_main.main(["channel", "search", "--source", "cryptocompare", "--query", "btc", "--limit", "2"]), 0)
            self.assertIn("BTC", search_output.getvalue())

            sub_output = io.StringIO()
            with patch("fetchers.http.HttpFetcher.get_json", side_effect=fake_get_json), redirect_stdout(sub_output):
                self.assertEqual(cli_main.main(["sub", "add", "--source", "cryptocompare", "--channel", "BTC"]), 0)
            self.assertIn("Bitcoin (BTC)", sub_output.getvalue())

            update_output = io.StringIO()
            with patch("fetchers.http.HttpFetcher.get_json", side_effect=fake_get_json), redirect_stdout(update_output):
                self.assertEqual(cli_main.main(["content", "update", "--source", "cryptocompare", "--channel", "BTC", "--limit", "2"]), 0)
            self.assertIn("saved_count", update_output.getvalue())

            query_output = io.StringIO()
            with redirect_stdout(query_output):
                self.assertEqual(cli_main.main(["content", "query", "--source", "cryptocompare", "--channel", "BTC", "--limit", "2"]), 0)
            self.assertIn("BTC", query_output.getvalue())
            self.assertIn("70450.43", query_output.getvalue())

    def test_cli_cryptocompare_update_requires_subscription(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "cryptocompare.db")):
            with self.assertRaisesRegex(Exception, "not subscribed"):
                cli_main.main(["content", "update", "--source", "cryptocompare", "--channel", "BTC", "--limit", "1"])

    def test_cli_xiaohongshu_flow_behaves_like_user_operation(self) -> None:
        class FakeClient:
            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def search_users(self, keyword: str) -> dict:
                _ = keyword
                return SourceSmokeTests()._xiaohongshu_user_search_payload()

            def get_user_notes(self, user_id: str, *, cursor: str = "") -> dict:
                _ = user_id, cursor
                return SourceSmokeTests()._xiaohongshu_user_notes_payload()

        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "xiaohongshu.db")),
            patch("sources.xiaohongshu.source.XiaohongshuClient", FakeClient),
        ):
            config_output = io.StringIO()
            with redirect_stdout(config_output):
                self.assertEqual(
                    cli_main.main(
                        [
                            "config",
                            "source",
                            "set",
                            "xiaohongshu",
                            "cookie",
                            "a1=token; web_session=session",
                        ]
                    ),
                    0,
                )

            search_output = io.StringIO()
            with redirect_stdout(search_output):
                self.assertEqual(
                    cli_main.main(["channel", "search", "--source", "xiaohongshu", "--query", "openai", "--limit", "2"]),
                    0,
                )
            self.assertIn("user/user-1", search_output.getvalue())

            sub_output = io.StringIO()
            with redirect_stdout(sub_output):
                self.assertEqual(
                    cli_main.main(
                        ["sub", "add", "--source", "xiaohongshu", "--channel", "user/user-1", "--name", "OpenAI研究员"]
                    ),
                    0,
                )
            self.assertIn("OpenAI研究员", sub_output.getvalue())

            update_output = io.StringIO()
            with redirect_stdout(update_output):
                self.assertEqual(
                    cli_main.main(["content", "update", "--source", "xiaohongshu", "--channel", "user/user-1", "--limit", "2"]),
                    0,
                )
            self.assertIn("saved_count", update_output.getvalue())

            query_output = io.StringIO()
            with redirect_stdout(query_output):
                self.assertEqual(
                    cli_main.main(
                        [
                            "content",
                            "query",
                            "--source",
                            "xiaohongshu",
                            "--channel",
                            "user/user-1",
                            "--limit",
                            "2",
                            "--jsonl",
                        ]
                    ),
                    0,
                )
            self.assertIn('"title": "第一行标题"', query_output.getvalue())
            self.assertIn('"author": "作者A"', query_output.getvalue())

    def test_cli_xiaohongshu_update_requires_subscription(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "xiaohongshu.db")):
            self.assertEqual(
                cli_main.main(
                    [
                        "config",
                        "source",
                        "set",
                        "xiaohongshu",
                        "cookie",
                        "a1=token; web_session=session",
                    ]
                ),
                0,
            )
            with self.assertRaisesRegex(Exception, "not subscribed"):
                cli_main.main(["content", "update", "--source", "xiaohongshu", "--channel", "user/user-1", "--limit", "1"])


if __name__ == "__main__":
    unittest.main()
