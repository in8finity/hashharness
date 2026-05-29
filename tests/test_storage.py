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

    def test_find_tips_bulk_returns_tip_per_id_and_null_for_missing(self) -> None:
        wp1_latest = self.store.create_item(
            item_type="Evidence",
            text="wp1 first",
            title="wp1-first",
            work_package_id="wp-1",
        )
        wp1_latest = self.store.create_item(
            item_type="Evidence",
            text="wp1 second",
            title="wp1-second",
            work_package_id="wp-1",
        )
        wp2_only = self.store.create_item(
            item_type="Evidence",
            text="wp2 only",
            title="wp2-only",
            work_package_id="wp-2",
        )
        self.store.create_item(
            item_type="HypothesisChange",
            text="wp1 hypothesis",
            title="wp1-hyp",
            work_package_id="wp-1",
        )
        self.store.flush_writes()

        result = self.store.find_tips_bulk(
            ["wp-1", "wp-2", "wp-missing", "wp-1"],
            "Evidence",
        )

        self.assertEqual(set(result.keys()), {"wp-1", "wp-2", "wp-missing"})
        self.assertEqual(result["wp-1"]["title"], "wp1-second")
        self.assertEqual(result["wp-1"]["text_sha256"], wp1_latest["text_sha256"])
        self.assertEqual(result["wp-2"]["text_sha256"], wp2_only["text_sha256"])
        self.assertIsNone(result["wp-missing"])

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

    def test_create_item_persists_synchronously(self) -> None:
        # I1 cross-process enforcement requires the duplicate check + insert
        # to be atomic at the storage layer. The previous "returns before
        # background flush" optimization left a window where two instances
        # could both pass the duplicate check and both write — a silent
        # last-writer-wins. The new contract: by the time create_item
        # returns, the row is durable.
        with TemporaryDirectory() as tempdir:
            store = TextStore(tempdir, clock=lambda: self.now)
            store.set_schema({"types": {"Evidence": {"links": {}}}})
            item = store.create_item(
                item_type="Evidence",
                text="sync write",
                title="Sync Write",
                work_package_id="wp-1",
            )
            item_path = store.items_dir / f"{item['text_sha256']}.json"
            self.assertTrue(item_path.exists())


class ChainPredecessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.wall = AdvancingClock()
        self.store = TextStore(self.tempdir.name, now_fn=self.wall)
        self.store.set_schema(
            {
                "types": {
                    "Evidence": {"links": {}},
                    "HypothesisChange": {
                        "links": {
                            "prevHypothesisChange": {
                                "kind": "single",
                                "target_types": ["HypothesisChange"],
                                "chain_predecessor": True,
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

    def _create_change(self, *, text: str, title: str, prev: str | None = None) -> dict:
        links: dict[str, object] = {}
        if prev is not None:
            links["prevHypothesisChange"] = prev
        return self.store.create_item(
            item_type="HypothesisChange",
            text=text,
            title=title,
            work_package_id="wp-1",
            links=links,
        )

    def test_first_item_must_omit_predecessor(self) -> None:
        first = self._create_change(text="c1", title="C1")
        # Subsequent first-item-style write (with prev omitted) is rejected because
        # the chain already has a head.
        with self.assertRaises(StorageError) as ctx:
            self._create_change(text="c1b", title="C1B")
        self.assertIn("must equal current head", str(ctx.exception))
        self.assertEqual(self.store._get_head("wp-1", "HypothesisChange"), first["record_sha256"])

    def test_first_item_rejects_supplied_predecessor(self) -> None:
        # Bootstrap a chain in another work package so we have a valid HypothesisChange
        # record to point at — isolates the "first item must omit prev" check from the
        # generic "link target must exist" / type validation.
        other = self.store.create_item(
            item_type="HypothesisChange",
            text="other-wp first",
            title="Other",
            work_package_id="wp-other",
            links={},
        )
        with self.assertRaises(StorageError) as ctx:
            self._create_change(text="c1", title="C1", prev=other["record_sha256"])
        self.assertIn("must be omitted", str(ctx.exception))

    def test_head_advances_on_successful_appends(self) -> None:
        c1 = self._create_change(text="c1", title="C1")
        c2 = self._create_change(text="c2", title="C2", prev=c1["record_sha256"])
        c3 = self._create_change(text="c3", title="C3", prev=c2["record_sha256"])
        self.assertEqual(self.store._get_head("wp-1", "HypothesisChange"), c3["record_sha256"])

    def test_fork_attempt_is_rejected(self) -> None:
        c1 = self._create_change(text="c1", title="C1")
        c2 = self._create_change(text="c2", title="C2", prev=c1["record_sha256"])
        # Attacker tries to fork off c1 after c2 has advanced the head.
        with self.assertRaises(StorageError) as ctx:
            self._create_change(text="c2-attacker", title="Fork", prev=c1["record_sha256"])
        self.assertIn("head moved", str(ctx.exception))
        # Head is still c2.
        self.assertEqual(self.store._get_head("wp-1", "HypothesisChange"), c2["record_sha256"])

    def test_find_tip_returns_head_not_latest_created_at(self) -> None:
        c1 = self._create_change(text="c1", title="C1")
        c2 = self._create_change(text="c2", title="C2", prev=c1["record_sha256"])
        tip = self.store.find_tip("wp-1", "HypothesisChange")
        self.assertEqual(tip["record_sha256"], c2["record_sha256"])

    def test_head_persists_across_store_reopen(self) -> None:
        c1 = self._create_change(text="c1", title="C1")
        c2 = self._create_change(text="c2", title="C2", prev=c1["record_sha256"])
        self.store.flush_writes()

        reopened = TextStore(self.tempdir.name, now_fn=AdvancingClock())
        try:
            self.assertEqual(
                reopened._get_head("wp-1", "HypothesisChange"),
                c2["record_sha256"],
            )
            # And appending must reference c2, not c1.
            with self.assertRaises(StorageError):
                reopened.create_item(
                    item_type="HypothesisChange",
                    text="rogue",
                    title="Rogue",
                    work_package_id="wp-1",
                    links={"prevHypothesisChange": c1["record_sha256"]},
                )
        finally:
            reopened.flush_writes()

    def test_schema_rejects_chain_predecessor_on_many_link(self) -> None:
        with self.assertRaises(StorageError) as ctx:
            self.store.set_schema(
                {
                    "types": {
                        "Bad": {
                            "links": {
                                "prevs": {
                                    "kind": "many",
                                    "target_types": ["Bad"],
                                    "chain_predecessor": True,
                                }
                            }
                        }
                    }
                }
            )
        self.assertIn("chain_predecessor", str(ctx.exception))

    def test_schema_rejects_two_chain_predecessor_links_on_one_type(self) -> None:
        with self.assertRaises(StorageError) as ctx:
            self.store.set_schema(
                {
                    "types": {
                        "Bad": {
                            "links": {
                                "a": {
                                    "kind": "single",
                                    "target_types": ["Bad"],
                                    "chain_predecessor": True,
                                },
                                "b": {
                                    "kind": "single",
                                    "target_types": ["Bad"],
                                    "chain_predecessor": True,
                                },
                            }
                        }
                    }
                }
            )
        self.assertIn("at most one", str(ctx.exception))

    def test_create_item_rejects_persisted_schema_with_two_chain_predecessors(self) -> None:
        from hashharness.storage import sha256_json

        malformed = {
            "types": {
                "X": {
                    "links": {
                        "prevA": {
                            "kind": "single",
                            "target_types": ["X"],
                            "chain_predecessor": True,
                        },
                        "prevB": {
                            "kind": "single",
                            "target_types": ["X"],
                            "chain_predecessor": True,
                        },
                    }
                }
            }
        }
        payload_sha = sha256_json(malformed)
        created_at = "2026-05-07T00:00:00+00:00"
        prev_head = self.store._backend_get_schema_head()
        record_sha = self.store._schema_record_sha256(
            prev_schema_sha256=prev_head,
            payload_sha256=payload_sha,
            created_at=created_at,
        )
        self.store._backend_persist_schema_version(
            {
                "record_sha256": record_sha,
                "prev_schema_sha256": prev_head,
                "payload_sha256": payload_sha,
                "created_at": created_at,
                "payload": malformed,
            }
        )
        self.store._backend_set_schema_head(record_sha, expected_prev=prev_head)

        with self.assertRaises(StorageError) as ctx:
            self.store.create_item(
                item_type="X",
                text="first",
                title="a",
                work_package_id="wp-1",
            )
        self.assertIn("at most one", str(ctx.exception))


class SchemaVersioningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.wall = AdvancingClock()
        self.store = TextStore(self.tempdir.name, now_fn=self.wall)
        self.schema_v1 = {"types": {"Evidence": {"links": {}}}}
        self.schema_v2 = {
            "types": {
                "Evidence": {"links": {}},
                "Note": {"links": {}},
            }
        }

    def tearDown(self) -> None:
        self.store.flush_writes()
        self.tempdir.cleanup()

    def test_genesis_schema_chain(self) -> None:
        v1 = self.store.set_schema(self.schema_v1)
        self.assertIsNone(v1["prev_schema_sha256"])
        self.assertEqual(self.store.get_schema_head(), v1["record_sha256"])

    def test_set_schema_requires_expected_prev_after_genesis(self) -> None:
        self.store.set_schema(self.schema_v1)
        with self.assertRaises(StorageError) as ctx:
            self.store.set_schema(self.schema_v2)  # missing expected_prev
        self.assertIn("Schema head moved", str(ctx.exception))

    def test_set_schema_appends_with_correct_expected_prev(self) -> None:
        v1 = self.store.set_schema(self.schema_v1)
        v2 = self.store.set_schema(self.schema_v2, expected_prev=v1["record_sha256"])
        self.assertEqual(v2["prev_schema_sha256"], v1["record_sha256"])
        self.assertEqual(self.store.get_schema_head(), v2["record_sha256"])

    def test_set_schema_rejects_stale_expected_prev(self) -> None:
        v1 = self.store.set_schema(self.schema_v1)
        self.store.set_schema(self.schema_v2, expected_prev=v1["record_sha256"])
        with self.assertRaises(StorageError) as ctx:
            self.store.set_schema(
                {"types": {"Evidence": {"links": {}}}},
                expected_prev=v1["record_sha256"],  # stale
            )
        self.assertIn("Schema head moved", str(ctx.exception))

    def test_create_item_stamps_current_schema_head(self) -> None:
        v1 = self.store.set_schema(self.schema_v1)
        item = self.store.create_item(
            item_type="Evidence",
            text="t",
            title="T",
            work_package_id="wp-1",
        )
        self.assertEqual(item["schema_sha256"], v1["record_sha256"])
        # Binding is sha(record_sha256, schema_sha256).
        self.assertNotEqual(item["schema_binding_sha256"], item["record_sha256"])

    def test_create_item_requires_schema_to_be_set(self) -> None:
        with self.assertRaises(StorageError) as ctx:
            self.store.create_item(
                item_type="Evidence",
                text="t",
                title="T",
                work_package_id="wp-1",
            )
        self.assertIn("No schema set", str(ctx.exception))

    def test_item_validates_against_schema_at_write_time(self) -> None:
        v1 = self.store.set_schema(self.schema_v1)
        item = self.store.create_item(
            item_type="Evidence",
            text="t",
            title="T",
            work_package_id="wp-1",
        )
        # Bump schema; item still binds to v1 and verify_chain still passes.
        self.store.set_schema(self.schema_v2, expected_prev=v1["record_sha256"])
        report = self.store.verify_chain(item["text_sha256"])
        self.assertTrue(report["ok"], report)
        self.assertEqual(report["items"][0]["schema_sha256"], v1["record_sha256"])

    def test_verify_chain_detects_schema_binding_tampering(self) -> None:
        self.store.set_schema(self.schema_v1)
        item = self.store.create_item(
            item_type="Evidence",
            text="t",
            title="T",
            work_package_id="wp-1",
        )
        self.store.flush_writes()
        path = self.store.items_dir / f"{item['text_sha256']}.json"
        tampered = json.loads(path.read_text(encoding="utf-8"))
        tampered["schema_binding_sha256"] = "0" * 64
        path.write_text(json.dumps(tampered, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        # Force a fresh read from disk; cache otherwise hides the tamper.
        self.store._drop_work_package_cache("wp-1")

        report = self.store.verify_chain(item["text_sha256"])
        self.assertFalse(report["ok"])
        self.assertTrue(
            any("schema_binding_sha256" in err for err in report["items"][0]["errors"])
        )

    def test_get_schema_history_returns_chain_in_order(self) -> None:
        v1 = self.store.set_schema(self.schema_v1)
        v2 = self.store.set_schema(self.schema_v2, expected_prev=v1["record_sha256"])
        history = self.store.get_schema_history()
        self.assertEqual(
            [v["record_sha256"] for v in history],
            [v1["record_sha256"], v2["record_sha256"]],
        )

    def test_get_schema_at_historical_sha(self) -> None:
        v1 = self.store.set_schema(self.schema_v1)
        self.store.set_schema(self.schema_v2, expected_prev=v1["record_sha256"])
        # Current head returns v2.
        self.assertIn("Note", self.store.get_schema()["types"])
        # Historical lookup returns v1.
        historical = self.store.get_schema(at=v1["record_sha256"])
        self.assertNotIn("Note", historical["types"])

    def test_legacy_data_is_migrated_to_genesis(self) -> None:
        # Simulate a pre-versioning store by writing the legacy schema.json
        # and an item directly to disk, then re-opening.
        with TemporaryDirectory() as legacy_root:
            legacy_root_path = Path(legacy_root)
            (legacy_root_path / "items").mkdir(parents=True, exist_ok=True)
            (legacy_root_path / "schema.json").write_text(
                json.dumps({"types": {"Evidence": {"links": {}}}}),
                encoding="utf-8",
            )
            legacy_item = {
                "type": "Evidence",
                "text_sha256": sha256_text("legacy"),
                "meta_sha256": "deadbeef" * 8,
                "links_sha256": "cafef00d" * 8,
                "record_sha256": "abcd1234" * 8,
                "work_package_id": "wp-1",
                "created_at": "2026-04-25T10:00:00+00:00",
                "title": "Legacy",
                "attributes": {},
                "text": "legacy",
                "links": {},
            }
            (legacy_root_path / "items" / f"{legacy_item['text_sha256']}.json").write_text(
                json.dumps(legacy_item, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            store = TextStore(legacy_root_path, now_fn=AdvancingClock())
            try:
                head = store.get_schema_head()
                self.assertIsNotNone(head)
                # schema.json is gone after migration; HEAD file took its place.
                self.assertFalse((legacy_root_path / "schema.json").exists())
                # Item was stamped with schema_sha256 + schema_binding_sha256.
                fetched = store.get_item(legacy_item["text_sha256"])
                self.assertEqual(fetched["schema_sha256"], head)
                self.assertIn("schema_binding_sha256", fetched)
            finally:
                store.flush_writes()


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
        self.assertIn("find_tips_bulk", {tool["name"] for tool in tools["result"]["tools"]})

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

    def test_http_transport_create_item_rejects_caller_supplied_created_at(self) -> None:
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

        response = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "create_item",
                    "arguments": {
                        "type": "Evidence",
                        "work_package_id": "wp-1",
                        "title": "Backdate Attempt",
                        "text": "claimed-old-evidence",
                        "created_at": "2020-01-01T00:00:00+00:00",
                    },
                },
            },
        )

        result = response["result"]
        self.assertTrue(result.get("isError"), result)
        self.assertIn("server-stamped", result["content"][0]["text"])

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

    def test_http_transport_find_tips_bulk_returns_dict_with_nulls(self) -> None:
        schema = {"types": {"Evidence": {"links": {}}}}
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "set_schema", "arguments": {"schema": schema}},
            },
        )
        for wp_id, title in [("wp-a", "A1"), ("wp-a", "A2"), ("wp-b", "B1")]:
            self._post_json(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "create_item",
                        "arguments": {
                            "type": "Evidence",
                            "work_package_id": wp_id,
                            "title": title,
                            "text": title,
                            "return": "full",
                        },
                    },
                },
            )

        result = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "find_tips_bulk",
                    "arguments": {
                        "work_package_ids": ["wp-a", "wp-b", "wp-missing"],
                        "type": "Evidence",
                        "fields": ["title", "text_sha256"],
                    },
                },
            },
        )

        tips = result["result"]["structuredContent"]["tips"]
        self.assertEqual(tips["wp-a"]["title"], "A2")
        self.assertEqual(tips["wp-b"]["title"], "B1")
        self.assertIsNone(tips["wp-missing"])
        self.assertEqual(set(tips["wp-a"].keys()), {"title", "text_sha256"})

    def test_http_transport_find_tips_bulk_rejects_oversize_input(self) -> None:
        schema = {"types": {"Evidence": {"links": {}}}}
        self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "set_schema", "arguments": {"schema": schema}},
            },
        )
        result = self._post_json(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "find_tips_bulk",
                    "arguments": {
                        "work_package_ids": [f"wp-{i}" for i in range(10001)],
                        "type": "Evidence",
                    },
                },
            },
        )
        self.assertTrue(result["result"].get("isError"))
        self.assertIn("10000", result["result"]["content"][0]["text"])

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

    def test_find_tips_bulk_non_chain_type(self) -> None:
        self.store.create_item(
            item_type="Evidence",
            text="wp1 first",
            title="wp1-first",
            work_package_id="wp-1",
        )
        wp1_latest = self.store.create_item(
            item_type="Evidence",
            text="wp1 second",
            title="wp1-second",
            work_package_id="wp-1",
        )
        wp2_only = self.store.create_item(
            item_type="Evidence",
            text="wp2 only",
            title="wp2-only",
            work_package_id="wp-2",
        )
        self.store.flush_writes()

        result = self.store.find_tips_bulk(
            ["wp-1", "wp-2", "wp-missing"], "Evidence"
        )

        self.assertEqual(result["wp-1"]["text_sha256"], wp1_latest["text_sha256"])
        self.assertEqual(result["wp-2"]["text_sha256"], wp2_only["text_sha256"])
        self.assertIsNone(result["wp-missing"])

    def test_find_tips_bulk_chain_type_uses_head_pointer(self) -> None:
        c1_a = self.store.create_item(
            item_type="HypothesisChange",
            text="wp-a c1",
            title="A1",
            work_package_id="wp-a",
        )
        c2_a = self.store.create_item(
            item_type="HypothesisChange",
            text="wp-a c2",
            title="A2",
            work_package_id="wp-a",
            links={"prevHypothesisChange": c1_a["record_sha256"]},
        )
        c1_b = self.store.create_item(
            item_type="HypothesisChange",
            text="wp-b c1",
            title="B1",
            work_package_id="wp-b",
        )
        self.store.flush_writes()

        result = self.store.find_tips_bulk(
            ["wp-a", "wp-b", "wp-empty"], "HypothesisChange"
        )

        self.assertEqual(result["wp-a"]["record_sha256"], c2_a["record_sha256"])
        self.assertEqual(result["wp-b"]["record_sha256"], c1_b["record_sha256"])
        self.assertIsNone(result["wp-empty"])

    def test_find_tips_bulk_empty_input(self) -> None:
        self.assertEqual(self.store.find_tips_bulk([], "Evidence"), {})

    def test_find_tips_bulk_chunks_large_input(self) -> None:
        wp_ids = [f"wp-{i:04d}" for i in range(750)]
        for wp_id in wp_ids:
            self.store.create_item(
                item_type="Evidence",
                text=f"evidence for {wp_id}",
                title=wp_id,
                work_package_id=wp_id,
            )
        self.store.flush_writes()

        result = self.store.find_tips_bulk(wp_ids + ["wp-missing"], "Evidence")

        self.assertEqual(len(result), len(wp_ids) + 1)
        self.assertIsNone(result["wp-missing"])
        self.assertEqual(result["wp-0000"]["title"], "wp-0000")
        self.assertEqual(result["wp-0749"]["title"], "wp-0749")

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

    def test_items_table_has_record_sha256_column_and_index(self) -> None:
        self.store.create_item(
            item_type="Evidence",
            text="indexed evidence",
            title="Evidence",
            work_package_id="wp-1",
            attributes={"author": "alice"},  # non-empty meta → record≠text sha
        )
        self.store.flush_writes()
        with self.store.db_lock:
            columns = {
                row[1]
                for row in self.store.conn.execute(
                    "PRAGMA table_info(items)"
                ).fetchall()
            }
            indexes = {
                row[1]
                for row in self.store.conn.execute(
                    "PRAGMA index_list(items)"
                ).fetchall()
            }
            row = self.store.conn.execute(
                "SELECT record_sha256, payload FROM items WHERE record_sha256 IS NOT NULL"
            ).fetchone()
        self.assertIn("record_sha256", columns)
        self.assertIn("items_record_sha256", indexes)
        # Column value equals the payload's record_sha256.
        self.assertEqual(row[0], json.loads(row[1])["record_sha256"])

    def test_find_tips_bulk_resolves_via_record_sha256_column(self) -> None:
        # Build a chain per work package so head ≠ first record, and confirm
        # the tip returned is the actual head (resolved by record_sha256, not
        # by scanning/decoding the whole chain).
        for wp in ("wp-a", "wp-b"):
            first = self.store.create_item(
                item_type="Evidence", text=f"{wp}-1", title=f"{wp}-1",
                work_package_id=wp,
            )
            self.store.create_item(
                item_type="Evidence", text=f"{wp}-2", title=f"{wp}-2",
                work_package_id=wp,
            )
            del first
        self.store.flush_writes()
        result = self.store.find_tips_bulk(["wp-a", "wp-b", "wp-missing"], "Evidence")
        self.assertIsNone(result["wp-missing"])
        # Non-chain type: tip is latest by created_at; both heads resolve.
        self.assertEqual(result["wp-a"]["work_package_id"], "wp-a")
        self.assertEqual(result["wp-b"]["work_package_id"], "wp-b")

    def test_legacy_rows_without_record_sha256_are_backfilled_on_reopen(self) -> None:
        item = self.store.create_item(
            item_type="Evidence",
            text="legacy row",
            title="Legacy",
            work_package_id="wp-1",
            attributes={"k": "v"},
        )
        self.store.flush_writes()
        db_path = self.store.db_path
        # Simulate a pre-column DB: blank the record_sha256 column.
        with self.store.db_lock:
            self.store.conn.execute("UPDATE items SET record_sha256 = NULL")
        self.store.close()

        reopened = SqliteTextStore(db_path, now_fn=AdvancingClock())
        try:
            with reopened.db_lock:
                stored = reopened.conn.execute(
                    "SELECT record_sha256 FROM items WHERE text_sha256 = ?",
                    (item["text_sha256"],),
                ).fetchone()[0]
            self.assertEqual(stored, item["record_sha256"])
            # And resolution by record_sha256 works post-backfill.
            resolved = reopened._backend_find_item_by_record_sha256(
                item["record_sha256"]
            )
            self.assertIsNotNone(resolved)
            self.assertEqual(resolved["text_sha256"], item["text_sha256"])
        finally:
            reopened.close()


class SchemaCASRaceTests(unittest.TestCase):
    """Cross-instance / cross-process CAS protection for set_schema."""

    def _race(self, store_factory, db_arg):
        # Bootstrap a genesis schema via a third store instance.
        boot = store_factory(db_arg)
        v0 = boot.set_schema({"types": {"A": {"links": {}}}})
        head0 = v0["record_sha256"]
        boot.flush_writes()
        if hasattr(boot, "close"):
            boot.close()

        s1 = store_factory(db_arg)
        s2 = store_factory(db_arg)

        # Force the interleave: both reads complete before either write.
        both_have_read = threading.Barrier(2)
        s1_done = threading.Event()
        orig_get_s1 = s1._backend_get_schema_head
        orig_get_s2 = s2._backend_get_schema_head

        def s1_get():
            h = orig_get_s1()
            both_have_read.wait(timeout=5)
            return h

        def s2_get():
            h = orig_get_s2()
            both_have_read.wait(timeout=5)
            s1_done.wait(timeout=5)
            return h

        s1._backend_get_schema_head = s1_get
        s2._backend_get_schema_head = s2_get

        out: dict = {}

        def go(label, store, schema):
            try:
                out[label] = store.set_schema(schema, expected_prev=head0)
            except StorageError as exc:
                out[label] = exc
            if label == "s1":
                s1_done.set()

        t1 = threading.Thread(
            target=go,
            args=("s1", s1, {"types": {"A": {"links": {}}, "B": {"links": {}}}}),
        )
        t2 = threading.Thread(
            target=go,
            args=("s2", s2, {"types": {"A": {"links": {}}, "C": {"links": {}}}}),
        )
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        s1.flush_writes()
        s2.flush_writes()
        if hasattr(s1, "close"):
            s1.close()
        if hasattr(s2, "close"):
            s2.close()

        return out

    def _assert_one_winner(self, out, store) -> None:
        successes = [v for v in out.values() if not isinstance(v, StorageError)]
        failures = [v for v in out.values() if isinstance(v, StorageError)]
        self.assertEqual(len(successes), 1, msg=f"both succeeded: {out}")
        self.assertEqual(len(failures), 1, msg=f"both failed: {out}")
        self.assertIn("Schema head moved", str(failures[0]))
        # Head points at the winner.
        self.assertEqual(
            store._backend_get_schema_head(),
            successes[0]["record_sha256"],
        )
        # Walk-back from head is linear (no fork on the active chain).
        history = store.get_schema_history()
        seen_prevs: set[str | None] = set()
        for version in history:
            prev = version["prev_schema_sha256"]
            self.assertNotIn(prev, seen_prevs)
            seen_prevs.add(prev)

    def test_sqlite_set_schema_races_resolve_to_one_winner(self) -> None:
        with TemporaryDirectory() as td:
            db = Path(td) / "h.sqlite"
            out = self._race(SqliteTextStore, db)
            verifier = SqliteTextStore(db)
            try:
                self._assert_one_winner(out, verifier)
            finally:
                verifier.close()

    def test_filesystem_set_schema_races_resolve_to_one_winner(self) -> None:
        with TemporaryDirectory() as td:
            out = self._race(lambda root: TextStore(root, now_fn=AdvancingClock()), td)
            verifier = TextStore(td)
            self._assert_one_winner(out, verifier)


class ItemChainCASRaceTests(unittest.TestCase):
    """Cross-instance / cross-process CAS protection for chain_predecessor heads (I4b)."""

    SCHEMA = {
        "types": {
            "X": {
                "links": {
                    "prev": {
                        "kind": "single",
                        "target_types": ["X"],
                        "chain_predecessor": True,
                    }
                }
            }
        }
    }

    def _race(self, store_factory, db_arg):
        boot = store_factory(db_arg)
        boot.set_schema(self.SCHEMA)
        first = boot.create_item(
            item_type="X", text="genesis", title="g", work_package_id="wp-1"
        )
        head0 = first["record_sha256"]
        boot.flush_writes()
        if hasattr(boot, "close"):
            boot.close()

        s1 = store_factory(db_arg)
        s2 = store_factory(db_arg)
        # Drop per-instance head caches so the patched _backend_get_head fires.
        s1.heads.clear()
        s2.heads.clear()

        barrier = threading.Barrier(2)
        s1_done = threading.Event()
        orig1, orig2 = s1._backend_get_head, s2._backend_get_head

        def g1(*a, **k):
            h = orig1(*a, **k)
            barrier.wait(timeout=5)
            return h

        def g2(*a, **k):
            h = orig2(*a, **k)
            barrier.wait(timeout=5)
            s1_done.wait(timeout=5)
            return h

        s1._backend_get_head = g1
        s2._backend_get_head = g2

        out: dict = {}

        def go(label, store, text):
            try:
                out[label] = store.create_item(
                    item_type="X",
                    text=text,
                    title=label,
                    work_package_id="wp-1",
                    links={"prev": head0},
                )
            except StorageError as exc:
                out[label] = exc
            if label == "s1":
                s1_done.set()

        t1 = threading.Thread(target=go, args=("s1", s1, "fork-A"))
        t2 = threading.Thread(target=go, args=("s2", s2, "fork-B"))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        s1.flush_writes()
        s2.flush_writes()
        if hasattr(s1, "close"):
            s1.close()
        if hasattr(s2, "close"):
            s2.close()
        return out, head0

    def _assert_one_winner(self, out, head0, store) -> None:
        successes = [v for v in out.values() if not isinstance(v, StorageError)]
        failures = [v for v in out.values() if isinstance(v, StorageError)]
        self.assertEqual(len(successes), 1, msg=f"both succeeded: {out}")
        self.assertEqual(len(failures), 1, msg=f"both failed: {out}")
        self.assertIn("Chain head moved", str(failures[0]))
        self.assertEqual(
            store._backend_get_head("wp-1", "X"),
            successes[0]["record_sha256"],
        )

    def test_sqlite_create_item_races_resolve_to_one_winner(self) -> None:
        with TemporaryDirectory() as td:
            db = Path(td) / "h.sqlite"
            out, head0 = self._race(SqliteTextStore, db)
            verifier = SqliteTextStore(db)
            try:
                self._assert_one_winner(out, head0, verifier)
            finally:
                verifier.close()

    def test_filesystem_create_item_races_resolve_to_one_winner(self) -> None:
        with TemporaryDirectory() as td:
            out, head0 = self._race(
                lambda root: TextStore(root, now_fn=AdvancingClock()), td
            )
            verifier = TextStore(td)
            self._assert_one_winner(out, head0, verifier)


class SchemaPinningTests(unittest.TestCase):
    """I5c: create_item must validate against the same schema version it stamps."""

    def test_create_item_pins_schema_across_concurrent_set_schema(self) -> None:
        with TemporaryDirectory() as td:
            db = Path(td) / "h.sqlite"
            boot = SqliteTextStore(db)
            v0 = boot.set_schema(
                {
                    "types": {
                        "X": {
                            "links": {
                                "a": {"kind": "single", "target_types": ["X"]}
                            }
                        }
                    }
                }
            )
            head0 = v0["record_sha256"]
            target = boot.create_item(
                item_type="X", text="target", title="t", work_package_id="wp-1"
            )
            boot.flush_writes()
            boot.close()

            writer = SqliteTextStore(db)
            caller = SqliteTextStore(db)

            # Force a concurrent set_schema to land between the caller's two
            # schema reads in create_item.
            orig_get_head = caller._backend_get_schema_head
            call_no = {"n": 0}

            def patched():
                call_no["n"] += 1
                h = orig_get_head()
                if call_no["n"] == 1:
                    writer.set_schema(
                        {
                            "types": {
                                "X": {
                                    "links": {
                                        "b": {
                                            "kind": "single",
                                            "target_types": ["X"],
                                        }
                                    }
                                }
                            }
                        },
                        expected_prev=head0,
                    )
                return h

            caller._backend_get_schema_head = patched

            # Old schema (S0) only knows link 'a'. With the fix, the caller
            # pins to S0; supplying link 'b' must be rejected as unknown.
            with self.assertRaises(StorageError) as ctx:
                caller.create_item(
                    item_type="X",
                    text="racy",
                    title="racy",
                    work_package_id="wp-1",
                    links={"b": target["record_sha256"]},
                )
            self.assertIn("Unknown link fields: b", str(ctx.exception))

            writer.flush_writes()
            caller.flush_writes()
            writer.close()
            caller.close()


class SchemaChainReachabilityTests(unittest.TestCase):
    """I5d: verify_chain rejects records bound to off-chain schema versions."""

    def test_verify_rejects_off_chain_schema_binding(self) -> None:
        from hashharness.storage import sha256_json, sha256_text

        with TemporaryDirectory() as td:
            db = Path(td) / "h.sqlite"
            store = SqliteTextStore(db)
            store.set_schema(
                {
                    "types": {
                        "X": {
                            "links": {
                                "a": {"kind": "single", "target_types": ["X"]}
                            }
                        }
                    }
                }
            )
            target = store.create_item(
                item_type="X", text="target", title="t", work_package_id="wp-1"
            )

            # Forge an off-chain schema version: self-consistent (correct
            # payload_sha256 + record_sha256), but its prev_schema_sha256
            # points at nothing reachable from the canonical head.
            rogue_payload = {
                "types": {
                    "X": {
                        "links": {
                            "a": {"kind": "single", "target_types": ["X"]},
                            "evil": {"kind": "single", "target_types": ["X"]},
                        }
                    }
                }
            }
            rogue_payload_sha = sha256_json(rogue_payload)
            rogue_record_sha = store._schema_record_sha256(
                prev_schema_sha256="00" * 32,
                payload_sha256=rogue_payload_sha,
                created_at="2099-01-01T00:00:00+00:00",
            )
            store._backend_persist_schema_version(
                {
                    "record_sha256": rogue_record_sha,
                    "prev_schema_sha256": "00" * 32,
                    "payload_sha256": rogue_payload_sha,
                    "created_at": "2099-01-01T00:00:00+00:00",
                    "payload": rogue_payload,
                }
            )

            # Persist a record bound to the off-chain schema, using the link
            # only the rogue schema knows about. All hashes computed honestly.
            text = "rogue-record"
            text_hash = sha256_text(text)
            created_at = "2099-01-01T00:00:01+00:00"
            meta_sha = store._meta_sha256(
                item_type="X",
                work_package_id="wp-1",
                created_at=created_at,
                title="r",
                attributes={},
            )
            links = {"evil": target["record_sha256"]}
            links_sha = sha256_json(links)
            record_sha = store._record_sha256(
                text_sha256=text_hash,
                meta_sha256=meta_sha,
                links_sha256=links_sha,
            )
            binding = store._schema_binding_sha256(
                record_sha256=record_sha, schema_sha256=rogue_record_sha
            )
            store._persist_item(
                {
                    "type": "X",
                    "text_sha256": text_hash,
                    "meta_sha256": meta_sha,
                    "links_sha256": links_sha,
                    "record_sha256": record_sha,
                    "schema_sha256": rogue_record_sha,
                    "schema_binding_sha256": binding,
                    "work_package_id": "wp-1",
                    "created_at": created_at,
                    "title": "r",
                    "attributes": {},
                    "text": text,
                    "links": links,
                }
            )
            store.flush_writes()
            store.close()

            verifier = SqliteTextStore(db)
            try:
                rep = verifier.verify_chain(text_hash)
                self.assertFalse(rep["ok"])
                # Find the rogue record's report and check the message.
                rogue_report = next(
                    r for r in rep["items"] if r["record_sha256"] == record_sha
                )
                self.assertTrue(
                    any(
                        "not in the canonical schema chain" in msg
                        for msg in rogue_report["errors"]
                    ),
                    msg=f"errors: {rogue_report['errors']}",
                )
            finally:
                verifier.close()


class TextHashUniqueRaceTests(unittest.TestCase):
    """I1: text_sha256 unique-and-immutable, even under cross-instance races."""

    def _race(self, store_factory, db_arg):
        boot = store_factory(db_arg)
        boot.set_schema(
            {"types": {"X": {"links": {}}, "Y": {"links": {}}}}
        )
        boot.flush_writes()
        if hasattr(boot, "close"):
            boot.close()

        s1 = store_factory(db_arg)
        s2 = store_factory(db_arg)
        barrier = threading.Barrier(2)
        s1_done = threading.Event()
        o1, o2 = s1._backend_read_item, s2._backend_read_item

        def r1(*a, **k):
            v = o1(*a, **k)
            barrier.wait(timeout=5)
            return v

        def r2(*a, **k):
            v = o2(*a, **k)
            barrier.wait(timeout=5)
            s1_done.wait(timeout=5)
            return v

        s1._backend_read_item = r1
        s2._backend_read_item = r2
        out: dict = {}

        def go(label, store, item_type, title, wp):
            try:
                out[label] = store.create_item(
                    item_type=item_type,
                    text="same-text",
                    title=title,
                    work_package_id=wp,
                )
            except StorageError as exc:
                out[label] = exc
            if label == "s1":
                s1_done.set()

        t1 = threading.Thread(target=go, args=("s1", s1, "X", "first", "wp-A"))
        t2 = threading.Thread(target=go, args=("s2", s2, "Y", "second", "wp-B"))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)
        s1.flush_writes()
        s2.flush_writes()
        if hasattr(s1, "close"):
            s1.close()
        if hasattr(s2, "close"):
            s2.close()
        return out

    def _assert_one_winner(self, out, store) -> None:
        successes = [v for v in out.values() if not isinstance(v, StorageError)]
        failures = [v for v in out.values() if isinstance(v, StorageError)]
        self.assertEqual(len(successes), 1, msg=f"both succeeded: {out}")
        self.assertEqual(len(failures), 1, msg=f"both failed: {out}")
        self.assertIn("same text sha256 already exists", str(failures[0]))
        winner = successes[0]
        persisted = store._backend_read_item(winner["text_sha256"])
        self.assertEqual(persisted["record_sha256"], winner["record_sha256"])

    def test_sqlite_create_item_races_resolve_to_one_winner(self) -> None:
        with TemporaryDirectory() as td:
            db = Path(td) / "h.sqlite"
            out = self._race(SqliteTextStore, db)
            verifier = SqliteTextStore(db)
            try:
                self._assert_one_winner(out, verifier)
            finally:
                verifier.close()

    def test_filesystem_create_item_races_resolve_to_one_winner(self) -> None:
        with TemporaryDirectory() as td:
            out = self._race(lambda root: TextStore(root, now_fn=AdvancingClock()), td)
            verifier = TextStore(td)
            self._assert_one_winner(out, verifier)


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

    def test_migrate_rejects_malformed_source_schema(self) -> None:
        from hashharness.migrate import migrate
        from hashharness.storage import sha256_json

        with TemporaryDirectory() as src_dir, TemporaryDirectory() as dst_dir:
            fs_store = TextStore(src_dir, now_fn=AdvancingClock())
            malformed = {
                "types": {
                    "X": {
                        "links": {
                            "prevA": {
                                "kind": "single",
                                "target_types": ["X"],
                                "chain_predecessor": True,
                            },
                            "prevB": {
                                "kind": "single",
                                "target_types": ["X"],
                                "chain_predecessor": True,
                            },
                        }
                    }
                }
            }
            payload_sha = sha256_json(malformed)
            created_at = "2026-05-07T00:00:00+00:00"
            record_sha = fs_store._schema_record_sha256(
                prev_schema_sha256=None,
                payload_sha256=payload_sha,
                created_at=created_at,
            )
            fs_store._backend_persist_schema_version(
                {
                    "record_sha256": record_sha,
                    "prev_schema_sha256": None,
                    "payload_sha256": payload_sha,
                    "created_at": created_at,
                    "payload": malformed,
                }
            )
            fs_store._backend_set_schema_head(record_sha, expected_prev=None)
            fs_store.flush_writes()

            db_path = Path(dst_dir) / "out.sqlite"
            with self.assertRaises(StorageError) as ctx:
                migrate(Path(src_dir), db_path)
            self.assertIn("at most one", str(ctx.exception))

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


class VerifyWorkPackageTests(unittest.TestCase):
    """list_work_packages + verify_work_package: scoped, whole-wp integrity."""

    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _store(self, backend: str):
        if backend == "fs":
            store = TextStore(self.tempdir.name, now_fn=AdvancingClock())
        else:
            store = SqliteTextStore(
                f"{self.tempdir.name}/h.sqlite", now_fn=AdvancingClock()
            )
        store.set_schema({"types": {"Evidence": {"links": {}}}})
        return store

    def _close(self, store) -> None:
        store.flush_writes()
        if isinstance(store, SqliteTextStore):
            store.close()

    def _list_run(self, backend: str) -> None:
        store = self._store(backend)
        try:
            store.create_item(item_type="Evidence", text="a", title="a", work_package_id="proj-1")
            store.create_item(item_type="Evidence", text="b", title="b", work_package_id="proj-2")
            store.create_item(item_type="Evidence", text="c", title="c", work_package_id="other-1")
            store.flush_writes()
            self.assertEqual(
                store.list_work_packages(), ["other-1", "proj-1", "proj-2"]
            )
            self.assertEqual(
                store.list_work_packages(prefix="proj-"), ["proj-1", "proj-2"]
            )
            self.assertEqual(store.list_work_packages(prefix="none-"), [])
        finally:
            self._close(store)

    def test_list_work_packages_filesystem(self) -> None:
        self._list_run("fs")

    def test_list_work_packages_sqlite(self) -> None:
        self._list_run("sqlite")

    def test_verify_work_package_clean_and_summary(self) -> None:
        store = self._store("sqlite")
        try:
            for t in ("a", "b", "c"):
                store.create_item(item_type="Evidence", text=t, title=t, work_package_id="wp-1")
            store.flush_writes()
            full = store.verify_work_package("wp-1")
            self.assertTrue(full["ok"])
            self.assertEqual(full["checked_items"], 3)
            self.assertEqual(len(full["items"]), 3)
            summ = store.verify_work_package("wp-1", summary=True)
            self.assertTrue(summ["ok"])
            self.assertEqual(summ["checked_items"], 3)
            self.assertEqual(summ["errors_count"], 0)
            self.assertNotIn("items", summ)
            # Empty / unknown wp verifies vacuously.
            self.assertEqual(store.verify_work_package("wp-none")["checked_items"], 0)
            self.assertTrue(store.verify_work_package("wp-none")["ok"])
        finally:
            self._close(store)

    def test_verify_work_package_catches_orphan_that_root_walk_misses(self) -> None:
        # Three independent (unlinked) records in one wp. verify_chain rooted at
        # one of them only checks that one; verify_work_package checks all three,
        # so tampering an *unreferenced* record is caught only by the latter.
        store = self._store("sqlite")
        try:
            a = store.create_item(item_type="Evidence", text="a", title="a", work_package_id="wp-1")
            b = store.create_item(item_type="Evidence", text="b", title="b", work_package_id="wp-1")
            store.create_item(item_type="Evidence", text="c", title="c", work_package_id="wp-1")
            store.flush_writes()

            # Tamper b's metadata on disk without updating its hashes.
            with store.db_lock:
                row = store.conn.execute(
                    "SELECT payload FROM items WHERE text_sha256 = ?", (b["text_sha256"],)
                ).fetchone()
                doc = json.loads(row[0])
                doc["title"] = "TAMPERED"
                store.conn.execute(
                    "UPDATE items SET payload = ? WHERE text_sha256 = ?",
                    (json.dumps(doc), b["text_sha256"]),
                )
            # Drop cache so the tampered payload is re-read from the backend.
            store.work_package_cache.clear()

            # Root walk from a clean, unrelated record: passes (never visits b).
            self.assertTrue(store.verify_chain(a["text_sha256"])["ok"])
            # Whole-wp verify: catches it.
            report = store.verify_work_package("wp-1")
            self.assertFalse(report["ok"])
            self.assertEqual(report["checked_items"], 3)
            bad = [it for it in report["items"] if not it["ok"]]
            self.assertEqual(len(bad), 1)
            self.assertEqual(bad[0]["text_sha256"], b["text_sha256"])
        finally:
            self._close(store)

    def test_verify_via_mcp_batch(self) -> None:
        store = self._store("sqlite")
        try:
            store.create_item(item_type="Evidence", text="a", title="a", work_package_id="wp-1")
            store.create_item(item_type="Evidence", text="b", title="b", work_package_id="wp-2")
            store.flush_writes()
            app = MCPApplication(store)
            listing = app._call_tool(
                {"name": "list_work_packages", "arguments": {}}
            )["structuredContent"]["work_package_ids"]
            self.assertEqual(listing, ["wp-1", "wp-2"])
            out = app._call_tool(
                {
                    "name": "verify_work_package",
                    "arguments": {"work_package_ids": listing, "summary": True},
                }
            )["structuredContent"]
            self.assertTrue(out["ok"])
            self.assertEqual(out["checked_work_packages"], 2)
        finally:
            self._close(store)


class WalCheckpointTests(unittest.TestCase):
    """WAL stays bounded: periodic TRUNCATE checkpoint shrinks the -wal file."""

    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _store(self, **kw) -> SqliteTextStore:
        store = SqliteTextStore(
            f"{self.tempdir.name}/h.sqlite", now_fn=AdvancingClock(), **kw
        )
        store.set_schema({"types": {"Evidence": {"links": {}}}})
        return store

    def _wal_size(self, store) -> int:
        wal = Path(f"{store.db_path}-wal")
        return wal.stat().st_size if wal.exists() else 0

    def _write(self, store, n: int) -> None:
        for i in range(n):
            store.create_item(
                item_type="Evidence", text=f"e{i}", title=f"e{i}",
                work_package_id=f"wp-{i}",
            )

    def test_periodic_truncate_keeps_wal_bounded(self) -> None:
        store = self._store(wal_checkpoint_writes=5)
        try:
            self._write(store, 20)  # 4 truncate cycles, last lands on a boundary
            store.flush_writes()
            # No open reader → TRUNCATE succeeds → -wal back to 0 bytes.
            self.assertEqual(self._wal_size(store), 0)
        finally:
            store.close()

    def test_wal_grows_when_periodic_truncate_disabled(self) -> None:
        store = self._store(wal_checkpoint_writes=0, wal_autocheckpoint_pages=0)
        try:
            self._write(store, 20)
            store.flush_writes()
            self.assertGreater(self._wal_size(store), 0)
            # Manual checkpoint still works and truncates.
            store.checkpoint()
            self.assertEqual(self._wal_size(store), 0)
        finally:
            store.close()

    def test_checkpoint_returns_triple_and_never_raises(self) -> None:
        store = self._store(wal_checkpoint_writes=0)
        try:
            self._write(store, 3)
            result = store.checkpoint()
            self.assertEqual(len(result), 3)
            self.assertTrue(all(isinstance(x, int) for x in result))
        finally:
            store.close()


class TipProjectionTests(unittest.TestCase):
    """find_tips_where: O(open-work) tip lookup via the maintained projection."""

    CHAIN_SCHEMA = {
        "types": {
            "TaskStatus": {
                "links": {
                    "prev": {
                        "kind": "single",
                        "target_types": ["TaskStatus"],
                        "chain_predecessor": True,
                    }
                }
            }
        }
    }

    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _make(self, backend: str):
        if backend == "fs":
            store = TextStore(self.tempdir.name, now_fn=AdvancingClock())
        else:
            store = SqliteTextStore(
                f"{self.tempdir.name}/h.sqlite", now_fn=AdvancingClock()
            )
        store.set_schema(self.CHAIN_SCHEMA)
        return store

    def _advance(self, store, wp: str, status: str) -> dict:
        head = store._get_head(wp, "TaskStatus")
        links = {} if head is None else {"prev": head}
        return store.create_item(
            item_type="TaskStatus",
            text=f"{wp}:{status}:{head or 'genesis'}",
            title=status,
            work_package_id=wp,
            attributes={"status": status},
            links=links,
        )

    def _run(self, backend: str) -> None:
        store = self._make(backend)
        try:
            self._advance(store, "wp-open1", "new")
            self._advance(store, "wp-open2", "new")
            self._advance(store, "wp-done", "new")
            self._advance(store, "wp-done", "done")
            store.flush_writes()

            # Only the two open chains come back — caller did NOT enumerate ids.
            open_tips = store.find_tips_where("TaskStatus", {"status": "new"})
            self.assertEqual(set(open_tips), {"wp-open1", "wp-open2"})
            self.assertTrue(
                all(t["attributes"]["status"] == "new" for t in open_tips.values())
            )

            # Advancing an open chain drops it from the projection's "new" set.
            self._advance(store, "wp-open1", "working")
            store.flush_writes()
            open_after = store.find_tips_where("TaskStatus", {"status": "new"})
            self.assertEqual(set(open_after), {"wp-open2"})
            self.assertEqual(
                set(store.find_tips_where("TaskStatus", {"status": "done"})), {"wp-done"}
            )

            # Restriction to a candidate set intersects.
            restricted = store.find_tips_where(
                "TaskStatus", {"status": "new"}, work_package_ids=["wp-open2", "wp-done"]
            )
            self.assertEqual(set(restricted), {"wp-open2"})

            # Empty predicate is rejected.
            with self.assertRaises(StorageError):
                store.find_tips_where("TaskStatus", {})
        finally:
            store.flush_writes()
            if isinstance(store, SqliteTextStore):
                store.close()

    def test_find_tips_where_filesystem(self) -> None:
        self._run("fs")

    def test_find_tips_where_sqlite(self) -> None:
        self._run("sqlite")

    def test_projection_rebuilt_on_reopen(self) -> None:
        store = self._make("sqlite")
        self._advance(store, "wp-1", "new")
        self._advance(store, "wp-2", "new")
        self._advance(store, "wp-2", "done")
        store.flush_writes()
        db_path = store.db_path
        # Simulate a pre-projection DB: drop the table entirely.
        with store.db_lock:
            store.conn.execute("DROP TABLE tip_attributes")
        store.close()

        reopened = SqliteTextStore(db_path, now_fn=AdvancingClock())
        try:
            # Rebuilt from the authoritative heads table on open.
            self.assertEqual(
                set(reopened.find_tips_where("TaskStatus", {"status": "new"})), {"wp-1"}
            )
            self.assertEqual(
                set(reopened.find_tips_where("TaskStatus", {"status": "done"})), {"wp-2"}
            )
        finally:
            reopened.close()

    def test_find_tips_where_via_mcp(self) -> None:
        store = self._make("sqlite")
        self._advance(store, "wp-a", "new")
        self._advance(store, "wp-b", "new")
        self._advance(store, "wp-b", "done")
        store.flush_writes()
        app = MCPApplication(store)
        try:
            out = app._call_tool(
                {
                    "name": "find_tips_where",
                    "arguments": {
                        "type": "TaskStatus",
                        "where_attributes": {"status": "new"},
                        "fields": ["work_package_id", "title"],
                    },
                }
            )
            tips = out["structuredContent"]["tips"]
            self.assertEqual(set(tips), {"wp-a"})
            self.assertEqual(tips["wp-a"]["title"], "new")
        finally:
            store.flush_writes()
            store.close()


class TipAttributeFilterTests(unittest.TestCase):
    """where_attributes filter on find_tip / find_tips_bulk, across backends and
    chain-predecessor / non-chain types."""

    CHAIN_SCHEMA = {
        "types": {
            "TaskStatus": {
                "links": {
                    "prev": {
                        "kind": "single",
                        "target_types": ["TaskStatus"],
                        "chain_predecessor": True,
                    }
                }
            }
        }
    }

    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _make(self, backend: str):
        if backend == "fs":
            store = TextStore(self.tempdir.name, now_fn=AdvancingClock())
        else:
            store = SqliteTextStore(
                f"{self.tempdir.name}/h.sqlite", now_fn=AdvancingClock()
            )
        store.set_schema(self.CHAIN_SCHEMA)
        return store

    def _advance(self, store, wp: str, status: str) -> dict:
        head = store._get_head(wp, "TaskStatus")
        links = {} if head is None else {"prev": head}
        return store.create_item(
            item_type="TaskStatus",
            text=f"{wp}:{status}:{store._get_head(wp, 'TaskStatus') or 'genesis'}",
            title=status,
            work_package_id=wp,
            attributes={"status": status},
            links=links,
        )

    def _run(self, backend: str) -> None:
        store = self._make(backend)
        try:
            # wp-open ends on `new`; wp-done ends on `done`.
            self._advance(store, "wp-open", "new")
            self._advance(store, "wp-done", "new")
            self._advance(store, "wp-done", "working")
            self._advance(store, "wp-done", "done")
            store.flush_writes()

            # find_tip: matching tip returned, non-matching raises.
            tip = store.find_tip("wp-open", "TaskStatus", where_attributes={"status": "new"})
            self.assertEqual(tip["attributes"]["status"], "new")
            with self.assertRaises(StorageError):
                store.find_tip("wp-done", "TaskStatus", where_attributes={"status": "new"})
            # No filter still returns the true head.
            self.assertEqual(
                store.find_tip("wp-done", "TaskStatus")["attributes"]["status"], "done"
            )

            # find_tips_bulk: only matching tips survive; rest map to null.
            res = store.find_tips_bulk(
                ["wp-open", "wp-done", "wp-missing"],
                "TaskStatus",
                where_attributes={"status": "new"},
            )
            self.assertEqual(res["wp-open"]["attributes"]["status"], "new")
            self.assertIsNone(res["wp-done"])
            self.assertIsNone(res["wp-missing"])

            # Without filter, both chains resolve to their real heads.
            res_all = store.find_tips_bulk(["wp-open", "wp-done"], "TaskStatus")
            self.assertEqual(res_all["wp-open"]["attributes"]["status"], "new")
            self.assertEqual(res_all["wp-done"]["attributes"]["status"], "done")
        finally:
            store.flush_writes()
            if isinstance(store, SqliteTextStore):
                store.close()

    def test_chain_tip_filter_filesystem(self) -> None:
        self._run("fs")

    def test_chain_tip_filter_sqlite(self) -> None:
        self._run("sqlite")

    def test_non_chain_tip_filter_sqlite(self) -> None:
        store = SqliteTextStore(f"{self.tempdir.name}/h2.sqlite", now_fn=AdvancingClock())
        store.set_schema({"types": {"Note": {"links": {}}}})
        try:
            store.create_item(
                item_type="Note", text="n1", title="n1",
                work_package_id="wp-1", attributes={"kind": "draft"},
            )
            store.create_item(
                item_type="Note", text="n2", title="n2",
                work_package_id="wp-1", attributes={"kind": "final"},
            )
            store.flush_writes()
            # Tip is the latest (final); filter for draft → null, for final → hit.
            self.assertIsNone(
                store.find_tips_bulk(["wp-1"], "Note", where_attributes={"kind": "draft"})["wp-1"]
            )
            self.assertEqual(
                store.find_tips_bulk(["wp-1"], "Note", where_attributes={"kind": "final"})["wp-1"]["title"],
                "n2",
            )
        finally:
            store.flush_writes()
            store.close()


if __name__ == "__main__":
    unittest.main()
