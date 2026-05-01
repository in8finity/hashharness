from __future__ import annotations

import json
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from hashharness.mcp_server import HttpMCPServer, MCPApplication
from hashharness.storage import (
    SqliteTextStore,
    StorageError,
    TextStore,
    make_store,
    sha256_joined,
    sha256_text,
)


class AdvancingClock:
    """Deterministic wall clock for tests; advances 1 second per call."""

    def __init__(self, start: datetime | None = None) -> None:
        self.current = start or datetime(2026, 4, 25, 10, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        result = self.current
        self.current = self.current + timedelta(seconds=1)
        return result


class TextStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.now = 0.0
        self.wall = AdvancingClock()
        self.store = TextStore(
            self.tempdir.name, clock=lambda: self.now, now_fn=self.wall
        )
        self.store.set_schema(
            {
                "types": {
                    "Evidence": {"links": {}},
                    "HypothesisChange": {
                        "links": {
                            "prevHypothesisChange": {
                                "kind": "single",
                                "target_types": ["HypothesisChange"],
                            },
                            "evidences": {
                                "kind": "many",
                                "target_types": ["Evidence"],
                            },
                        }
                    },
                }
            }
        )

    def tearDown(self) -> None:
        self.store.flush_writes()
        self.tempdir.cleanup()

    def test_create_and_fetch_by_hash(self) -> None:
        item = self.store.create_item(
            item_type="Evidence",
            text="lab note 1",
            title="Evidence 1",
            work_package_id="wp-1",
        )

        fetched = self.store.get_item(item["text_sha256"])
        self.assertEqual(fetched["text"], "lab note 1")
        self.assertEqual(fetched["text_sha256"], sha256_text("lab note 1"))
        self.assertEqual(fetched["attributes"], {})
        self.assertIn("meta_sha256", fetched)
        self.assertIn("links_sha256", fetched)
        self.assertIn("record_sha256", fetched)

    def test_create_item_persists_attributes(self) -> None:
        item = self.store.create_item(
            item_type="Evidence",
            text="lab note with metadata",
            title="Evidence With Attributes",
            work_package_id="wp-1",
            attributes={"author": "alice", "score": 3, "tags": ["lab", "sample"]},
        )

        self.assertEqual(
            item["attributes"],
            {"author": "alice", "score": 3, "tags": ["lab", "sample"]},
        )

    def test_create_with_typed_links_and_list_hash(self) -> None:
        evidence_a = self.store.create_item(
            item_type="Evidence",
            text="evidence a",
            title="A",
            work_package_id="wp-1",
        )
        evidence_b = self.store.create_item(
            item_type="Evidence",
            text="evidence b",
            title="B",
            work_package_id="wp-1",
        )
        previous = self.store.create_item(
            item_type="HypothesisChange",
            text="old change",
            title="Old",
            work_package_id="wp-1",
            links={},
        )

        item = self.store.create_item(
            item_type="HypothesisChange",
            text="new hypothesis",
            title="New",
            work_package_id="wp-1",
            links={
                "prevHypothesisChange": previous["record_sha256"],
                "evidences": [evidence_b["record_sha256"], evidence_a["record_sha256"]],
            },
        )

        self.assertEqual(item["links"]["prevHypothesisChange"], previous["record_sha256"])
        self.assertEqual(
            item["links"]["evidencesHash"],
            sha256_joined([evidence_b["record_sha256"], evidence_a["record_sha256"]]),
        )

    def test_find_items_by_substring(self) -> None:
        self.store.create_item(
            item_type="Evidence",
            text="contains critical anomaly",
            title="Anomaly",
            work_package_id="wp-9",
        )
        self.store.create_item(
            item_type="Evidence",
            text="boring baseline",
            title="Baseline",
            work_package_id="wp-9",
        )

        results = self.store.find_items(query="critical", field="text")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "Anomaly")

    def test_rejects_updates_for_same_text_hash(self) -> None:
        self.store.create_item(
            item_type="Evidence",
            text="same text",
            title="Original",
            work_package_id="wp-1",
        )

        with self.assertRaises(StorageError):
            self.store.create_item(
                item_type="Evidence",
                text="same text",
                title="Changed title",
                work_package_id="wp-2",
            )

    def test_rejects_wrong_link_type(self) -> None:
        evidence = self.store.create_item(
            item_type="Evidence",
            text="fact",
            title="Fact",
            work_package_id="wp-1",
        )

        with self.assertRaises(StorageError):
            self.store.create_item(
                item_type="HypothesisChange",
                text="bad refs",
                title="Bad",
                work_package_id="wp-1",
                links={"prevHypothesisChange": evidence["record_sha256"]},
            )

    def test_links_use_record_sha256_not_text_sha256(self) -> None:
        evidence = self.store.create_item(
            item_type="Evidence",
            text="fact",
            title="Fact",
            work_package_id="wp-1",
        )
        # Sanity: with non-empty meta, the two ids diverge.
        self.assertNotEqual(evidence["text_sha256"], evidence["record_sha256"])

        # Linking by record_sha256 succeeds.
        change = self.store.create_item(
            item_type="HypothesisChange",
            text="hypothesis",
            title="Hyp",
            work_package_id="wp-1",
            links={"evidences": [evidence["record_sha256"]]},
        )
        self.assertEqual(change["links"]["evidences"], [evidence["record_sha256"]])

        # Linking by text_sha256 is rejected (target lookup is by record_sha256).
        with self.assertRaises(StorageError):
            self.store.create_item(
                item_type="HypothesisChange",
                text="hypothesis by text",
                title="HypByText",
                work_package_id="wp-1",
                links={"evidences": [evidence["text_sha256"]]},
            )

    def test_created_at_is_server_stamped_and_monotonic(self) -> None:
        first = self.store.create_item(
            item_type="Evidence",
            text="first",
            title="First",
            work_package_id="wp-1",
        )
        second = self.store.create_item(
            item_type="Evidence",
            text="second",
            title="Second",
            work_package_id="wp-1",
        )
        # Server-stamped wall clock advances; both timestamps are well-formed UTC.
        self.assertTrue(first["created_at"].endswith("+00:00"))
        self.assertTrue(second["created_at"].endswith("+00:00"))
        self.assertLess(first["created_at"], second["created_at"])

    def test_rejects_non_object_attributes(self) -> None:
        with self.assertRaises(StorageError):
            self.store.create_item(
                item_type="Evidence",
                text="bad attrs",
                title="Bad Attrs",
                work_package_id="wp-1",
                attributes=["not", "an", "object"],
            )

    def test_verify_chain_checks_transitive_links(self) -> None:
        evidence = self.store.create_item(
            item_type="Evidence",
            text="fact",
            title="Fact",
            work_package_id="wp-1",
        )
        previous = self.store.create_item(
            item_type="HypothesisChange",
            text="old hypothesis",
            title="Old",
            work_package_id="wp-1",
        )
        current = self.store.create_item(
            item_type="HypothesisChange",
            text="current hypothesis",
            title="Current",
            work_package_id="wp-1",
            links={
                "prevHypothesisChange": previous["record_sha256"],
                "evidences": [evidence["record_sha256"]],
            },
        )

        report = self.store.verify_chain(current["text_sha256"])

        self.assertTrue(report["ok"])
        self.assertEqual(report["checked_items"], 3)

    def test_query_chain_returns_transitive_records(self) -> None:
        evidence = self.store.create_item(
            item_type="Evidence",
            text="fact",
            title="Fact",
            work_package_id="wp-1",
        )
        previous = self.store.create_item(
            item_type="HypothesisChange",
            text="old hypothesis",
            title="Old",
            work_package_id="wp-1",
        )
        current = self.store.create_item(
            item_type="HypothesisChange",
            text="current hypothesis",
            title="Current",
            work_package_id="wp-1",
            links={
                "prevHypothesisChange": previous["record_sha256"],
                "evidences": [evidence["record_sha256"]],
            },
        )

        result = self.store.query_chain(current["text_sha256"])

        self.assertEqual(result["item_count"], 3)
        self.assertEqual(result["root_text_sha256"], current["text_sha256"])
        self.assertEqual(
            {item["text_sha256"] for item in result["items"]},
            {
                current["text_sha256"],
                previous["text_sha256"],
                evidence["text_sha256"],
            },
        )

    def test_get_work_package_returns_all_records(self) -> None:
        evidence = self.store.create_item(
            item_type="Evidence",
            text="fact",
            title="Fact",
            work_package_id="wp-1",
        )
        change = self.store.create_item(
            item_type="HypothesisChange",
            text="current hypothesis",
            title="Current",
            work_package_id="wp-1",
            links={"evidences": [evidence["record_sha256"]]},
        )
        self.store.create_item(
            item_type="Evidence",
            text="other package",
            title="Other",
            work_package_id="wp-2",
        )

        result = self.store.get_work_package("wp-1")

        self.assertEqual(result["work_package_id"], "wp-1")
        self.assertEqual(result["item_count"], 2)
        self.assertEqual(
            {item["text_sha256"] for item in result["items"]},
            {evidence["text_sha256"], change["text_sha256"]},
        )
        self.assertIsNone(result["type_filter"])

    def test_get_work_package_filters_by_type(self) -> None:
        evidence = self.store.create_item(
            item_type="Evidence",
            text="fact",
            title="Fact",
            work_package_id="wp-1",
        )
        self.store.create_item(
            item_type="HypothesisChange",
            text="current hypothesis",
            title="Current",
            work_package_id="wp-1",
            links={"evidences": [evidence["record_sha256"]]},
        )

        result = self.store.get_work_package("wp-1", item_type="Evidence")

        self.assertEqual(result["item_count"], 1)
        self.assertEqual(result["type_filter"], "Evidence")
        self.assertEqual(result["items"][0]["type"], "Evidence")

    def test_find_items_can_filter_by_attributes(self) -> None:
        self.store.create_item(
            item_type="Evidence",
            text="status changed for user",
            title="Relevant",
            work_package_id="wp-1",
            attributes={"event": "status-changed", "kind": "audit"},
        )
        self.store.create_item(
            item_type="Evidence",
            text="different event",
            title="Irrelevant",
            work_package_id="wp-1",
            attributes={"event": "user-created", "kind": "audit"},
        )
        self.store.flush_writes()

        result = self.store.find_items(attributes={"event": "status-changed"})

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "Relevant")

    def test_find_tip_returns_latest_item_for_type(self) -> None:
        self.store.create_item(
            item_type="Evidence",
            text="older evidence",
            title="Older",
            work_package_id="wp-1",
        )
        latest = self.store.create_item(
            item_type="Evidence",
            text="latest evidence",
            title="Latest",
            work_package_id="wp-1",
        )
        self.store.create_item(
            item_type="HypothesisChange",
            text="other type",
            title="Other Type",
            work_package_id="wp-1",
        )
        self.store.flush_writes()

        result = self.store.find_tip("wp-1", "Evidence")

        self.assertEqual(result["text_sha256"], latest["text_sha256"])
        self.assertEqual(result["title"], "Latest")

    def test_find_items_ignores_empty_orphan_files(self) -> None:
        self.store.create_item(
            item_type="Evidence",
            text="valid evidence",
            title="Valid",
            work_package_id="wp-1",
        )
        self.store.flush_writes()
        (self.store.items_dir / ("0" * 64 + ".json")).write_text("", encoding="utf-8")

        result = self.store.find_items(query="valid", field="text")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "Valid")

    def test_work_package_cache_expires_after_five_minutes_of_inactivity(self) -> None:
        item = self.store.create_item(
            item_type="Evidence",
            text="cached evidence",
            title="Cached",
            work_package_id="wp-1",
        )
        self.store.flush_writes()
        self.assertIn("wp-1", self.store.work_package_cache)

        self.now = 301.0
        self.store.find_items(query="no-match", field="text")

        self.assertNotIn("wp-1", self.store.work_package_cache)
        self.assertNotIn(item["text_sha256"], self.store.item_to_work_package)

    def test_item_access_refreshes_work_package_cache_ttl(self) -> None:
        item_wp1 = self.store.create_item(
            item_type="Evidence",
            text="cached evidence",
            title="Cached",
            work_package_id="wp-1",
        )
        self.store.create_item(
            item_type="Evidence",
            text="other package item",
            title="Other",
            work_package_id="wp-2",
        )
        self.store.flush_writes()

        self.now = 299.0
        self.store.get_item(item_wp1["text_sha256"])
        self.now = 301.0
        self.store.find_items(query="no-match", field="text")

        self.assertIn("wp-1", self.store.work_package_cache)
        self.assertNotIn("wp-2", self.store.work_package_cache)

    def test_verify_chain_detects_tampering(self) -> None:
        item = self.store.create_item(
            item_type="Evidence",
            text="stable text",
            title="Stable",
            work_package_id="wp-1",
        )
        self.store.flush_writes()
        item_path = self.store.items_dir / f"{item['text_sha256']}.json"
        tampered = self.store.get_item(item["text_sha256"])
        tampered["title"] = "Tampered"
        item_path.write_text(
            json.dumps(tampered, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        report = self.store.verify_chain(item["text_sha256"])

        self.assertFalse(report["ok"])
        self.assertIn("meta_sha256 does not match item metadata", report["items"][0]["errors"])

    def test_verify_chain_accepts_legacy_record_without_attributes(self) -> None:
        legacy_item = {
            "type": "Evidence",
            "text_sha256": sha256_text("legacy text"),
            "meta_sha256": self.store._legacy_meta_sha256(
                item_type="Evidence",
                work_package_id="wp-1",
                created_at="2026-04-25T10:30:00+00:00",
                title="Legacy",
            ),
            "links_sha256": "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "record_sha256": self.store._record_sha256(
                text_sha256=sha256_text("legacy text"),
                meta_sha256=self.store._legacy_meta_sha256(
                    item_type="Evidence",
                    work_package_id="wp-1",
                    created_at="2026-04-25T10:30:00+00:00",
                    title="Legacy",
                ),
                links_sha256="44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            ),
            "work_package_id": "wp-1",
            "created_at": "2026-04-25T10:30:00+00:00",
            "title": "Legacy",
            "text": "legacy text",
            "links": {},
            "stored_at": "2026-04-25T10:31:00+00:00",
        }
        path = self.store.items_dir / f"{legacy_item['text_sha256']}.json"
        path.write_text(json.dumps(legacy_item, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        report = self.store.verify_chain(legacy_item["text_sha256"])

        self.assertTrue(report["ok"])
        self.assertEqual(report["checked_items"], 1)

    def test_create_item_returns_before_background_flush(self) -> None:
        class SlowPersistStore(TextStore):
            def __init__(self, *args: object, persist_gate: threading.Event, **kwargs: object) -> None:
                self.persist_gate = persist_gate
                super().__init__(*args, **kwargs)

            def _persist_item_to_disk(self, item_path: object, item: dict[str, object]) -> None:
                self.persist_gate.wait(timeout=5)
                super()._persist_item_to_disk(item_path, item)

        with TemporaryDirectory() as tempdir:
            gate = threading.Event()
            slow_store = SlowPersistStore(
                tempdir,
                clock=lambda: self.now,
                persist_gate=gate,
            )
            slow_store.set_schema(
                {
                    "types": {
                        "Evidence": {"links": {}},
                    }
                }
            )
            item = slow_store.create_item(
                item_type="Evidence",
                text="background write",
                title="Background Write",
                work_package_id="wp-1",
            )
            item_path = slow_store.items_dir / f"{item['text_sha256']}.json"

            self.assertFalse(item_path.exists())
            self.assertEqual(slow_store.get_item(item["text_sha256"])["title"], "Background Write")

            gate.set()
            slow_store.flush_writes()
            self.assertTrue(item_path.exists())
            tmp_files = list(slow_store.items_dir.glob(f".{item_path.name}.*.tmp"))
            self.assertEqual(tmp_files, [])


class HttpMCPServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.wall = AdvancingClock()
        self.store = TextStore(self.tempdir.name, now_fn=self.wall)
        self.server = HttpMCPServer(MCPApplication(self.store), "127.0.0.1", 8000)

    def tearDown(self) -> None:
        self.store.flush_writes()
        self.tempdir.cleanup()

    def test_http_transport_handles_initialize_and_tools(self) -> None:
        init = self._post_json(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        )
        tools = self._post_json(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        )

        self.assertEqual(init["result"]["serverInfo"]["name"], "hashharness")
        self.assertIn("query_chain", {tool["name"] for tool in tools["result"]["tools"]})
        self.assertIn("get_work_package", {tool["name"] for tool in tools["result"]["tools"]})
        self.assertIn("find_tip", {tool["name"] for tool in tools["result"]["tools"]})

    def test_http_transport_runs_tool_calls(self) -> None:
        schema = {
            "types": {
                "Evidence": {"links": {}},
                "HypothesisChange": {
                    "links": {
                        "prevHypothesisChange": {
                            "kind": "single",
                            "target_types": ["HypothesisChange"],
                        },
                        "evidences": {
                            "kind": "many",
                            "target_types": ["Evidence"],
                        },
                    }
                },
            }
        }
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "set_schema", "arguments": {"schema": schema}},
            },
        )
        evidence = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "Observation A",
                        "attributes": {"source": "lab"},
                        "text": "The sample changed color after heating.",
                        "links": {},
                        "return": "full",
                    },
                },
            },
        )
        evidence_hash = evidence["result"]["structuredContent"]["record_sha256"]
        current = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "HypothesisChange",
                        "work_package_id": "wp-1",
                        "title": "Current hypothesis",
                        "text": "Updated hypothesis.",
                        "links": {"evidences": [evidence_hash]},
                    },
                },
            },
        )
        current_hash = current["result"]["structuredContent"]["text_sha256"]

        queried = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "query_chain",
                    "arguments": {"text_sha256": current_hash},
                },
            },
        )

        self.assertEqual(queried["result"]["structuredContent"]["item_count"], 2)
        self.assertEqual(
            evidence["result"]["structuredContent"]["attributes"],
            {"source": "lab"},
        )

    def test_http_transport_create_item_defaults_to_minimal_return(self) -> None:
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "set_schema",
                    "arguments": {"schema": {"types": {"Evidence": {"links": {}}}}},
                },
            },
        )

        result = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "Observation A",
                        "text": "The sample changed color after heating.",
                    },
                },
            },
        )

        self.assertEqual(
            set(result["result"]["structuredContent"].keys()),
            {"record_sha256", "text_sha256"},
        )

    def test_http_transport_find_items_supports_fields_and_attributes(self) -> None:
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "set_schema",
                    "arguments": {"schema": {"types": {"Evidence": {"links": {}}}}},
                },
            },
        )
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "Observation A",
                        "text": "The sample changed color after heating.",
                        "attributes": {"event": "status-changed"},
                        "return": "full",
                    },
                },
            },
        )
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "Observation B",
                        "text": "Other event.",
                        "attributes": {"event": "user-created"},
                        "return": "full",
                    },
                },
            },
        )

        result = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "find_items",
                    "arguments": {
                        "attributes": {"event": "status-changed"},
                        "fields": ["title", "text_sha256", "attributes"],
                    },
                },
            },
        )

        self.assertEqual(result["result"]["structuredContent"]["items"][0]["title"], "Observation A")
        self.assertEqual(
            set(result["result"]["structuredContent"]["items"][0].keys()),
            {"attributes", "text_sha256", "title"},
        )

    def test_http_transport_verify_chain_summary(self) -> None:
        schema = {"types": {"Evidence": {"links": {}}}}
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "set_schema", "arguments": {"schema": schema}},
            },
        )
        created = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "Observation A",
                        "text": "The sample changed color after heating.",
                        "return": "full",
                    },
                },
            },
        )
        text_sha256 = created["result"]["structuredContent"]["text_sha256"]

        result = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "verify_chain",
                    "arguments": {"text_sha256": text_sha256, "summary": True},
                },
            },
        )

        self.assertEqual(
            set(result["result"]["structuredContent"].keys()),
            {"checked_items", "errors_count", "ok", "root_text_sha256"},
        )

    def test_http_transport_find_tip_returns_compact_item(self) -> None:
        schema = {"types": {"Evidence": {"links": {}}}}
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "set_schema", "arguments": {"schema": schema}},
            },
        )
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "Old",
                        "text": "old",
                        "return": "full",
                    },
                },
            },
        )
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "New",
                        "text": "new",
                        "return": "full",
                    },
                },
            },
        )

        result = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "find_tip",
                    "arguments": {"work_package_id": "wp-1", "type": "Evidence"},
                },
            },
        )

        self.assertEqual(result["result"]["structuredContent"]["title"], "New")
        self.assertEqual(
            set(result["result"]["structuredContent"].keys()),
            {"created_at", "record_sha256", "text_sha256", "title", "type"},
        )

    def test_http_transport_gets_work_package(self) -> None:
        schema = {
            "types": {
                "Evidence": {"links": {}},
                "HypothesisChange": {
                    "links": {
                        "evidences": {
                            "kind": "many",
                            "target_types": ["Evidence"],
                        },
                    }
                },
            }
        }
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "set_schema", "arguments": {"schema": schema}},
            },
        )
        evidence = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "Observation A",
                        "attributes": {"source": "lab"},
                        "text": "The sample changed color after heating.",
                        "links": {},
                    },
                },
            },
        )
        evidence_hash = evidence["result"]["structuredContent"]["record_sha256"]
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "HypothesisChange",
                        "work_package_id": "wp-1",
                        "title": "Current hypothesis",
                        "text": "Updated hypothesis.",
                        "links": {"evidences": [evidence_hash]},
                    },
                },
            },
        )

        result = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "get_work_package",
                    "arguments": {"work_package_id": "wp-1", "type": "Evidence"},
                },
            },
        )

        self.assertEqual(result["result"]["structuredContent"]["item_count"], 1)
        self.assertEqual(result["result"]["structuredContent"]["items"][0]["type"], "Evidence")

    def test_http_transport_accepts_notifications(self) -> None:
        status, body = self._post_json_raw(
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        )
        self.assertEqual(status, 202)
        self.assertEqual(body, b"")

    def test_http_transport_health_endpoint(self) -> None:
        status, headers, body = self.server.handle_http_request(method="GET", path="/health")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": True})

    def _post_json(self, payload: dict[str, object]) -> dict[str, object]:
        status, body = self._post_json_raw(payload)
        self.assertEqual(status, 200)
        return json.loads(body.decode("utf-8"))

    def _post_json_raw(self, payload: dict[str, object]) -> tuple[int, bytes]:
        data = json.dumps(payload).encode("utf-8")
        status, _, body = self.server.handle_http_request(
            method="POST",
            path="/mcp",
            headers={"Content-Type": "application/json"},
            body=data,
        )
        return int(status), body


class SqliteTextStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.now = 0.0
        self.wall = AdvancingClock()
        self.store = SqliteTextStore(
            f"{self.tempdir.name}/hashharness.sqlite",
            clock=lambda: self.now,
            now_fn=self.wall,
        )
        self.store.set_schema(
            {
                "types": {
                    "Evidence": {"links": {}},
                    "HypothesisChange": {
                        "links": {
                            "prevHypothesisChange": {
                                "kind": "single",
                                "target_types": ["HypothesisChange"],
                            },
                            "evidences": {
                                "kind": "many",
                                "target_types": ["Evidence"],
                            },
                        }
                    },
                }
            }
        )

    def tearDown(self) -> None:
        self.store.flush_writes()
        self.store.close()
        self.tempdir.cleanup()

    def test_create_and_fetch_round_trip(self) -> None:
        item = self.store.create_item(
            item_type="Evidence",
            text="sqlite-backed evidence",
            title="Evidence",
            work_package_id="wp-1",
            attributes={"author": "alice"},
        )
        self.store.flush_writes()

        fetched = self.store.get_item(item["text_sha256"])
        self.assertEqual(fetched["text"], "sqlite-backed evidence")
        self.assertEqual(fetched["text_sha256"], sha256_text("sqlite-backed evidence"))
        self.assertEqual(fetched["attributes"], {"author": "alice"})

    def test_persistence_survives_reopen(self) -> None:
        item = self.store.create_item(
            item_type="Evidence",
            text="durable evidence",
            title="Durable",
            work_package_id="wp-1",
        )
        self.store.flush_writes()
        db_path = self.store.db_path
        self.store.close()

        reopened = SqliteTextStore(db_path)
        try:
            fetched = reopened.get_item(item["text_sha256"])
            self.assertEqual(fetched["title"], "Durable")
            self.assertEqual(reopened.get_schema()["types"]["Evidence"], {"links": {}})
        finally:
            reopened.flush_writes()
            reopened.close()

    def test_find_items_and_work_package_lookup(self) -> None:
        evidence = self.store.create_item(
            item_type="Evidence",
            text="contains critical anomaly",
            title="Anomaly",
            work_package_id="wp-9",
        )
        self.store.create_item(
            item_type="Evidence",
            text="boring baseline",
            title="Baseline",
            work_package_id="wp-9",
        )
        self.store.create_item(
            item_type="HypothesisChange",
            text="hypothesis pointing at anomaly",
            title="Hyp",
            work_package_id="wp-9",
            links={"evidences": [evidence["record_sha256"]]},
        )
        self.store.flush_writes()

        results = self.store.find_items(query="critical", field="text")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "Anomaly")

        package = self.store.get_work_package("wp-9")
        self.assertEqual(package["item_count"], 3)

    def test_rejects_conflicting_rewrite(self) -> None:
        self.store.create_item(
            item_type="Evidence",
            text="same text",
            title="Original",
            work_package_id="wp-1",
        )
        self.store.flush_writes()
        # Drop cache to force the conflict path through the backend.
        self.store._drop_work_package_cache("wp-1")
        with self.assertRaises(StorageError):
            self.store.create_item(
                item_type="Evidence",
                text="same text",
                title="Different",
                work_package_id="wp-2",
            )

    def test_verify_chain_round_trip(self) -> None:
        evidence = self.store.create_item(
            item_type="Evidence",
            text="fact",
            title="Fact",
            work_package_id="wp-1",
        )
        change = self.store.create_item(
            item_type="HypothesisChange",
            text="current hypothesis",
            title="Current",
            work_package_id="wp-1",
            links={"evidences": [evidence["record_sha256"]]},
        )
        self.store.flush_writes()

        report = self.store.verify_chain(change["text_sha256"])
        self.assertTrue(report["ok"])
        self.assertEqual(report["checked_items"], 2)


class MigrateToolTests(unittest.TestCase):
    def test_migrate_filesystem_to_sqlite(self) -> None:
        from hashharness.migrate import migrate

        with TemporaryDirectory() as src_dir, TemporaryDirectory() as dst_dir:
            fs_store = TextStore(src_dir, now_fn=AdvancingClock())
            fs_store.set_schema(
                {
                    "types": {
                        "Evidence": {"links": {}},
                        "HypothesisChange": {
                            "links": {
                                "evidences": {
                                    "kind": "many",
                                    "target_types": ["Evidence"],
                                }
                            }
                        },
                    }
                }
            )
            evidence = fs_store.create_item(
                item_type="Evidence",
                text="lab note",
                title="Evidence",
                work_package_id="wp-1",
                attributes={"author": "alice"},
            )
            change = fs_store.create_item(
                item_type="HypothesisChange",
                text="hypothesis",
                title="Hyp",
                work_package_id="wp-1",
                links={"evidences": [evidence["record_sha256"]]},
            )
            fs_store.flush_writes()

            db_path = Path(dst_dir) / "out.sqlite"
            result = migrate(Path(src_dir), db_path, verify=True)
            self.assertEqual(result["items_copied"], 2)

            sqlite_store = SqliteTextStore(db_path)
            try:
                self.assertEqual(
                    sqlite_store.get_item(evidence["text_sha256"])["text"],
                    "lab note",
                )
                report = sqlite_store.verify_chain(change["text_sha256"])
                self.assertTrue(report["ok"])
                self.assertEqual(report["checked_items"], 2)
                self.assertEqual(
                    sqlite_store.get_schema()["types"]["Evidence"],
                    {"links": {}},
                )
            finally:
                sqlite_store.flush_writes()
                sqlite_store.close()

    def test_migrate_refuses_existing_destination(self) -> None:
        from hashharness.migrate import migrate

        with TemporaryDirectory() as src_dir, TemporaryDirectory() as dst_dir:
            TextStore(src_dir)  # create empty layout
            db_path = Path(dst_dir) / "out.sqlite"
            db_path.write_bytes(b"")
            with self.assertRaises(StorageError):
                migrate(Path(src_dir), db_path)


class MakeStoreTests(unittest.TestCase):
    def test_filesystem_factory(self) -> None:
        with TemporaryDirectory() as tempdir:
            store = make_store("filesystem", tempdir)
            self.assertIsInstance(store, TextStore)
            store.flush_writes()

    def test_sqlite_factory(self) -> None:
        with TemporaryDirectory() as tempdir:
            store = make_store("sqlite", f"{tempdir}/db.sqlite")
            self.assertIsInstance(store, SqliteTextStore)
            store.flush_writes()
            store.close()

    def test_unknown_backend_raises(self) -> None:
        with self.assertRaises(StorageError):
            make_store("redis", "/tmp/whatever")


if __name__ == "__main__":
    unittest.main()
