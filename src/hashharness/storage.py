from __future__ import annotations

import hashlib
import json
import queue
import re
import sqlite3
import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any


class StorageError(Exception):
    """Raised when the store rejects an operation."""


@dataclass(frozen=True)
class LinkRule:
    name: str
    kind: str
    target_types: tuple[str, ...]
    required: bool = False


@dataclass
class CachedWorkPackage:
    items_by_sha: dict[str, dict[str, Any]]
    last_used_at: float


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_joined(values: list[str]) -> str:
    payload = "\n".join(sorted(values))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class BaseTextStore:
    def __init__(
        self,
        *,
        cache_ttl_seconds: float = 300.0,
        clock: Any | None = None,
    ) -> None:
        self.cache_ttl_seconds = cache_ttl_seconds
        self.clock = clock or monotonic
        self.work_package_cache: dict[str, CachedWorkPackage] = {}
        self.item_to_work_package: dict[str, str] = {}
        self.cache_lock = threading.RLock()
        self.write_queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()
        self.pending_writes: set[str] = set()
        self.write_thread = threading.Thread(
            target=self._write_worker,
            name="hashharness-writer",
            daemon=True,
        )
        self._init_backend()
        self.write_thread.start()

    # ------------------------------------------------------------------ backend
    def _init_backend(self) -> None:
        raise NotImplementedError

    def _backend_get_schema(self) -> dict[str, Any] | None:
        raise NotImplementedError

    def _backend_set_schema(self, schema: dict[str, Any]) -> None:
        raise NotImplementedError

    def _backend_read_item(self, text_sha256: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def _backend_iter_items(self) -> Iterator[dict[str, Any]]:
        raise NotImplementedError

    def _backend_iter_items_for_work_package(
        self, work_package_id: str
    ) -> Iterator[dict[str, Any]]:
        raise NotImplementedError

    def _persist_item(self, item: dict[str, Any]) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------- public
    def get_schema(self) -> dict[str, Any]:
        schema = self._backend_get_schema()
        if schema is None:
            return {"types": {}}
        return schema

    def set_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        self._validate_schema_definition(schema)
        self._backend_set_schema(schema)
        return schema

    def get_item(self, text_sha256: str) -> dict[str, Any]:
        self._evict_expired_work_packages()
        cached_item = self._get_cached_item(text_sha256)
        if cached_item is not None:
            self._touch_work_package(cached_item["work_package_id"])
            return cached_item

        item = self._backend_read_item(text_sha256)
        if item is None:
            raise StorageError(f"Item not found for sha256={text_sha256}")
        self._get_or_load_work_package_cache(item["work_package_id"])
        with self.cache_lock:
            cached = self.work_package_cache[item["work_package_id"]]
        return cached.items_by_sha[text_sha256]

    def create_item(
        self,
        *,
        item_type: str,
        text: str,
        title: str,
        work_package_id: str,
        created_at: str,
        attributes: dict[str, Any] | None = None,
        links: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        schema = self.get_schema()
        rules = self._rules_for_type(schema, item_type)
        text_hash = sha256_text(text)

        validated_links = self._validate_links(rules, links or {})
        normalized_created_at = self._validate_datetime(created_at)
        validated_attributes = self._validate_attributes(attributes)
        meta_sha256 = self._meta_sha256(
            item_type=item_type,
            work_package_id=work_package_id,
            created_at=normalized_created_at,
            title=title,
            attributes=validated_attributes,
        )
        links_sha256 = sha256_json(validated_links)
        item = {
            "type": item_type,
            "text_sha256": text_hash,
            "meta_sha256": meta_sha256,
            "links_sha256": links_sha256,
            "record_sha256": self._record_sha256(
                text_sha256=text_hash,
                meta_sha256=meta_sha256,
                links_sha256=links_sha256,
            ),
            "work_package_id": work_package_id,
            "created_at": normalized_created_at,
            "title": title,
            "attributes": validated_attributes,
            "text": text,
            "links": validated_links,
            "stored_at": datetime.now(UTC).isoformat(),
        }

        # Hold cache_lock continuously across the duplicate check and the
        # insert so check-then-insert is atomic. Without this, two
        # concurrent create_item calls for the same text_sha256 (or for
        # different texts targeting the same work_package) can both pass
        # the duplicate check and both insert — last-writer-wins on the
        # cache slot, both callers return success. cache_lock is an RLock,
        # so reentrant acquisitions in _get_cached_item / _cache_item /
        # _enqueue_write / _touch_work_package / _get_or_load_work_package_cache
        # are fine.
        with self.cache_lock:
            cached_existing = self._get_cached_item(text_hash)
            if cached_existing:
                if self._same_item(cached_existing, item):
                    self._touch_work_package(cached_existing["work_package_id"])
                    return cached_existing
                raise StorageError(
                    "An item with the same text sha256 already exists and cannot be updated"
                )

            existing = self._backend_read_item(text_hash)
            if existing is not None:
                if self._same_item(existing, item):
                    self._get_or_load_work_package_cache(existing["work_package_id"])
                    return existing
                raise StorageError(
                    "An item with the same text sha256 already exists and cannot be updated"
                )

            self._cache_item(item)
            self._enqueue_write(item)
            return item

    def find_items(
        self,
        *,
        query: str | None = None,
        item_type: str | None = None,
        field: str = "text",
        regex: bool = False,
        limit: int = 20,
        attributes: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self._evict_expired_work_packages()
        if field not in {"text", "title", "work_package_id", "all"}:
            raise StorageError(f"Unsupported search field: {field}")

        normalized_query = query or ""
        matcher = re.compile(normalized_query, re.IGNORECASE) if regex and normalized_query else None
        results: list[dict[str, Any]] = []
        matched_work_packages: set[str] = set()
        seen_text_sha256: set[str] = set()

        for item in self._backend_iter_items():
            seen_text_sha256.add(item["text_sha256"])
            if item_type and item.get("type") != item_type:
                continue
            if attributes and not self._attributes_match(item.get("attributes", {}), attributes):
                continue
            haystacks = self._haystacks(item, field)
            matched = True if not normalized_query else any(
                matcher.search(candidate) is not None if matcher else normalized_query.lower() in candidate.lower()
                for candidate in haystacks
            )
            if matched:
                results.append(item)
                matched_work_packages.add(item["work_package_id"])
            if len(results) >= limit:
                break

        if len(results) < limit:
            with self.cache_lock:
                cached_items = [
                    item
                    for cached in self.work_package_cache.values()
                    for item in cached.items_by_sha.values()
                    if item["text_sha256"] not in seen_text_sha256
                ]
            for item in sorted(cached_items, key=lambda entry: entry["text_sha256"]):
                if item_type and item.get("type") != item_type:
                    continue
                if attributes and not self._attributes_match(item.get("attributes", {}), attributes):
                    continue
                haystacks = self._haystacks(item, field)
                matched = True if not normalized_query else any(
                    matcher.search(candidate) is not None if matcher else normalized_query.lower() in candidate.lower()
                    for candidate in haystacks
                )
                if matched:
                    results.append(item)
                    matched_work_packages.add(item["work_package_id"])
                if len(results) >= limit:
                    break
        for work_package_id in matched_work_packages:
            self._get_or_load_work_package_cache(work_package_id)
        return results

    def verify_chain(self, text_sha256: str) -> dict[str, Any]:
        checked: list[dict[str, Any]] = []
        seen: set[str] = set()

        def visit(current_text_sha256: str) -> None:
            if current_text_sha256 in seen:
                return
            seen.add(current_text_sha256)

            try:
                item = self.get_item(current_text_sha256)
            except StorageError as exc:
                checked.append(
                    {
                        "text_sha256": current_text_sha256,
                        "record_sha256": None,
                        "type": None,
                        "ok": False,
                        "errors": [str(exc)],
                        "referenced_hashes": [],
                    }
                )
                return
            item_report = self._verify_item(item)
            checked.append(item_report)

            for referenced_hash in item_report["referenced_hashes"]:
                if referenced_hash not in seen:
                    visit(referenced_hash)

        visit(text_sha256)
        ok = all(entry["ok"] for entry in checked)
        return {
            "root_text_sha256": text_sha256,
            "ok": ok,
            "checked_items": len(checked),
            "items": checked,
        }

    def query_chain(self, text_sha256: str) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        seen: set[str] = set()

        def visit(current_text_sha256: str) -> None:
            if current_text_sha256 in seen:
                return
            seen.add(current_text_sha256)

            item = self.get_item(current_text_sha256)
            items.append(item)

            schema = self.get_schema()
            rules = self._rules_for_type(schema, item.get("type", ""))
            for referenced_hash in self._extract_reference_hashes(item.get("links", {}), rules):
                visit(referenced_hash)

        visit(text_sha256)
        return {
            "root_text_sha256": text_sha256,
            "items": items,
            "item_count": len(items),
        }

    def get_work_package(
        self,
        work_package_id: str,
        *,
        item_type: str | None = None,
    ) -> dict[str, Any]:
        cached = self._get_or_load_work_package_cache(work_package_id)
        items = list(cached.items_by_sha.values())
        if item_type:
            items = [item for item in items if item.get("type") == item_type]

        return {
            "work_package_id": work_package_id,
            "type_filter": item_type,
            "items": items,
            "item_count": len(items),
        }

    def find_tip(self, work_package_id: str, item_type: str) -> dict[str, Any]:
        cached = self._get_or_load_work_package_cache(work_package_id)
        candidates = [
            item for item in cached.items_by_sha.values() if item.get("type") == item_type
        ]
        if not candidates:
            raise StorageError(
                f"No items found for work_package_id={work_package_id} and type={item_type}"
            )
        return max(
            candidates,
            key=lambda item: (
                datetime.fromisoformat(item["created_at"]),
                item["text_sha256"],
            ),
        )

    def flush_writes(self) -> None:
        self.write_queue.join()

    # -------------------------------------------------------------- internals
    def _haystacks(self, item: dict[str, Any], field: str) -> list[str]:
        if field == "all":
            return [
                item.get("title", ""),
                item.get("work_package_id", ""),
                item.get("text", ""),
            ]
        return [str(item.get(field, ""))]

    def _same_item(self, existing: dict[str, Any], candidate: dict[str, Any]) -> bool:
        comparable_existing = dict(existing)
        comparable_candidate = dict(candidate)
        comparable_existing.pop("stored_at", None)
        comparable_candidate.pop("stored_at", None)
        return comparable_existing == comparable_candidate

    def _evict_expired_work_packages(self) -> None:
        now = self.clock()
        with self.cache_lock:
            expired = [
                work_package_id
                for work_package_id, cached in self.work_package_cache.items()
                if now - cached.last_used_at > self.cache_ttl_seconds
                and not any(text_sha256 in self.pending_writes for text_sha256 in cached.items_by_sha)
            ]
        for work_package_id in expired:
            self._drop_work_package_cache(work_package_id)

    def _drop_work_package_cache(self, work_package_id: str) -> None:
        with self.cache_lock:
            cached = self.work_package_cache.pop(work_package_id, None)
            if not cached:
                return
            for text_sha256 in cached.items_by_sha:
                self.item_to_work_package.pop(text_sha256, None)

    def _touch_work_package(self, work_package_id: str) -> None:
        with self.cache_lock:
            cached = self.work_package_cache.get(work_package_id)
            if cached:
                cached.last_used_at = self.clock()

    def _get_or_load_work_package_cache(
        self, work_package_id: str, *, force_reload: bool = False
    ) -> CachedWorkPackage:
        self._evict_expired_work_packages()
        with self.cache_lock:
            if not force_reload and work_package_id in self.work_package_cache:
                self._touch_work_package(work_package_id)
                return self.work_package_cache[work_package_id]

        items = list(self._backend_iter_items_for_work_package(work_package_id))
        cached = CachedWorkPackage(
            items_by_sha={item["text_sha256"]: item for item in items},
            last_used_at=self.clock(),
        )
        with self.cache_lock:
            self._drop_work_package_cache(work_package_id)
            self.work_package_cache[work_package_id] = cached
            for text_sha256 in cached.items_by_sha:
                self.item_to_work_package[text_sha256] = work_package_id
        return cached

    def _get_cached_item(self, text_sha256: str) -> dict[str, Any] | None:
        with self.cache_lock:
            work_package_id = self.item_to_work_package.get(text_sha256)
            if not work_package_id:
                return None
            cached = self.work_package_cache.get(work_package_id)
            if not cached:
                return None
            return cached.items_by_sha.get(text_sha256)

    def _cache_item(self, item: dict[str, Any]) -> None:
        work_package_id = item["work_package_id"]
        cached = self._get_or_load_work_package_cache(work_package_id)
        with self.cache_lock:
            cached.items_by_sha[item["text_sha256"]] = item
            cached.last_used_at = self.clock()
            self.item_to_work_package[item["text_sha256"]] = work_package_id

    def _enqueue_write(self, item: dict[str, Any]) -> None:
        text_sha256 = item["text_sha256"]
        with self.cache_lock:
            if text_sha256 in self.pending_writes:
                return
            self.pending_writes.add(text_sha256)
        self.write_queue.put((text_sha256, item))

    def _write_worker(self) -> None:
        while True:
            text_sha256, item = self.write_queue.get()
            try:
                self._persist_item(item)
            finally:
                with self.cache_lock:
                    self.pending_writes.discard(text_sha256)
                self.write_queue.task_done()

    def _meta_sha256(
        self,
        *,
        item_type: str,
        work_package_id: str,
        created_at: str,
        title: str,
        attributes: dict[str, Any],
    ) -> str:
        return sha256_json(
            {
                "attributes": attributes,
                "created_at": created_at,
                "title": title,
                "type": item_type,
                "work_package_id": work_package_id,
            }
        )

    def _legacy_meta_sha256(
        self,
        *,
        item_type: str,
        work_package_id: str,
        created_at: str,
        title: str,
    ) -> str:
        return sha256_json(
            {
                "created_at": created_at,
                "title": title,
                "type": item_type,
                "work_package_id": work_package_id,
            }
        )

    def _record_sha256(
        self,
        *,
        text_sha256: str,
        meta_sha256: str,
        links_sha256: str,
    ) -> str:
        return sha256_json(
            {
                "links_sha256": links_sha256,
                "meta_sha256": meta_sha256,
                "text_sha256": text_sha256,
            }
        )

    def _validate_schema_definition(self, schema: dict[str, Any]) -> None:
        if not isinstance(schema, dict) or not isinstance(schema.get("types"), dict):
            raise StorageError("Schema must be an object with a 'types' mapping")

        for type_name, definition in schema["types"].items():
            if not isinstance(definition, dict):
                raise StorageError(f"Type definition for {type_name} must be an object")
            links = definition.get("links", {})
            if not isinstance(links, dict):
                raise StorageError(f"links for type {type_name} must be an object")
            for link_name, link_rule in links.items():
                self._parse_rule(link_name, link_rule)

    def _rules_for_type(self, schema: dict[str, Any], item_type: str) -> dict[str, LinkRule]:
        types = schema.get("types", {})
        if item_type not in types:
            raise StorageError(f"Type {item_type} is not defined in schema")
        raw_links = types[item_type].get("links", {})
        return {name: self._parse_rule(name, rule) for name, rule in raw_links.items()}

    def _parse_rule(self, name: str, rule: dict[str, Any]) -> LinkRule:
        if not isinstance(rule, dict):
            raise StorageError(f"Link rule {name} must be an object")
        kind = rule.get("kind")
        target_types = rule.get("target_types", [])
        required = bool(rule.get("required", False))
        if kind not in {"single", "many"}:
            raise StorageError(f"Link rule {name} has unsupported kind {kind}")
        if not isinstance(target_types, list) or not target_types or not all(
            isinstance(value, str) and value for value in target_types
        ):
            raise StorageError(f"Link rule {name} must declare non-empty target_types")
        return LinkRule(
            name=name,
            kind=kind,
            target_types=tuple(target_types),
            required=required,
        )

    def _validate_links(
        self, rules: dict[str, LinkRule], links: dict[str, Any]
    ) -> dict[str, Any]:
        if not isinstance(links, dict):
            raise StorageError("links must be an object")

        unknown = set(links) - set(rules)
        if unknown:
            names = ", ".join(sorted(unknown))
            raise StorageError(f"Unknown link fields: {names}")

        validated: dict[str, Any] = {}
        for name, rule in rules.items():
            if name not in links:
                if rule.required:
                    raise StorageError(f"Missing required link: {name}")
                continue

            value = links[name]
            if rule.kind == "single":
                if not isinstance(value, str):
                    raise StorageError(f"Link {name} must be a sha256 string")
                self._validate_target(name, value, rule)
                validated[name] = value
                continue

            if not isinstance(value, list) or not all(isinstance(entry, str) for entry in value):
                raise StorageError(f"Link {name} must be a list of sha256 strings")
            for entry in value:
                self._validate_target(name, entry, rule)
            validated[name] = value
            validated[f"{name}Hash"] = sha256_joined(value)

        return validated

    def _validate_target(self, link_name: str, text_sha256: str, rule: LinkRule) -> None:
        if not re.fullmatch(r"[0-9a-f]{64}", text_sha256):
            raise StorageError(f"Link {link_name} contains an invalid sha256: {text_sha256}")
        target = self.get_item(text_sha256)
        if target.get("type") not in rule.target_types:
            expected = ", ".join(rule.target_types)
            raise StorageError(
                f"Link {link_name} expects [{expected}] but got {target.get('type')}"
            )

    def _validate_datetime(self, value: str) -> str:
        if not isinstance(value, str):
            raise StorageError("created_at must be a datetime string")
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise StorageError("created_at must be a valid ISO 8601 datetime") from exc
        if parsed.tzinfo is None:
            raise StorageError("created_at must include a timezone offset")
        return parsed.isoformat()

    def _validate_attributes(self, attributes: dict[str, Any] | None) -> dict[str, Any]:
        if attributes is None:
            return {}
        if not isinstance(attributes, dict):
            raise StorageError("attributes must be an object")
        try:
            json.dumps(attributes, sort_keys=True, ensure_ascii=False)
        except TypeError as exc:
            raise StorageError("attributes must be JSON-serializable") from exc
        return attributes

    def _attributes_match(self, item_attributes: dict[str, Any], expected: dict[str, Any]) -> bool:
        if not isinstance(item_attributes, dict):
            return False
        for key, value in expected.items():
            if key not in item_attributes:
                return False
            if item_attributes[key] != value:
                return False
        return True

    def _verify_item(self, item: dict[str, Any]) -> dict[str, Any]:
        errors: list[str] = []
        referenced_hashes: list[str] = []

        expected_text_sha256 = sha256_text(item.get("text", ""))
        if item.get("text_sha256") != expected_text_sha256:
            errors.append("text_sha256 does not match text")

        try:
            normalized_created_at = self._validate_datetime(item.get("created_at"))
        except StorageError as exc:
            errors.append(str(exc))
            normalized_created_at = item.get("created_at")

        if "attributes" in item:
            expected_meta_sha256 = self._meta_sha256(
                item_type=item.get("type", ""),
                work_package_id=item.get("work_package_id", ""),
                created_at=normalized_created_at,
                title=item.get("title", ""),
                attributes=self._validate_attributes(item.get("attributes")),
            )
        else:
            expected_meta_sha256 = self._legacy_meta_sha256(
                item_type=item.get("type", ""),
                work_package_id=item.get("work_package_id", ""),
                created_at=normalized_created_at,
                title=item.get("title", ""),
            )
        if item.get("meta_sha256") != expected_meta_sha256:
            errors.append("meta_sha256 does not match item metadata")

        try:
            rules = self._rules_for_type(self.get_schema(), item.get("type", ""))
            raw_links = {
                name: item.get("links", {}).get(name)
                for name in rules
                if name in item.get("links", {})
            }
            expected_links = self._validate_links(rules, raw_links)
            if item.get("links") != expected_links:
                errors.append("stored links do not match validated links")
            referenced_hashes = self._extract_reference_hashes(expected_links, rules)
        except StorageError as exc:
            errors.append(str(exc))
            referenced_hashes = self._extract_reference_hashes_heuristic(item.get("links", {}))
            expected_links = item.get("links", {})

        expected_links_sha256 = sha256_json(expected_links)
        if item.get("links_sha256") != expected_links_sha256:
            errors.append("links_sha256 does not match links")

        expected_record_sha256 = self._record_sha256(
            text_sha256=expected_text_sha256,
            meta_sha256=expected_meta_sha256,
            links_sha256=expected_links_sha256,
        )
        if item.get("record_sha256") != expected_record_sha256:
            errors.append("record_sha256 does not match text/meta/links hashes")

        return {
            "text_sha256": item.get("text_sha256"),
            "record_sha256": item.get("record_sha256"),
            "type": item.get("type"),
            "ok": not errors,
            "errors": errors,
            "referenced_hashes": referenced_hashes,
        }

    def _extract_reference_hashes(
        self, links: dict[str, Any], rules: dict[str, LinkRule]
    ) -> list[str]:
        references: list[str] = []
        for name, rule in rules.items():
            if name not in links:
                continue
            value = links[name]
            if rule.kind == "single":
                references.append(value)
            else:
                references.extend(value)
        return references

    def _extract_reference_hashes_heuristic(self, links: dict[str, Any]) -> list[str]:
        references: list[str] = []
        for key, value in links.items():
            if key.endswith("Hash"):
                continue
            if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
                references.append(value)
            if isinstance(value, list):
                references.extend(
                    entry
                    for entry in value
                    if isinstance(entry, str) and re.fullmatch(r"[0-9a-f]{64}", entry)
                )
        return references


class FilesystemTextStore(BaseTextStore):
    def __init__(
        self,
        root: str | Path,
        *,
        cache_ttl_seconds: float = 300.0,
        clock: Any | None = None,
    ) -> None:
        self.root = Path(root)
        self.items_dir = self.root / "items"
        self.schema_path = self.root / "schema.json"
        super().__init__(cache_ttl_seconds=cache_ttl_seconds, clock=clock)

    def _init_backend(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.items_dir.mkdir(parents=True, exist_ok=True)

    def _backend_get_schema(self) -> dict[str, Any] | None:
        if not self.schema_path.exists():
            return None
        return json.loads(self.schema_path.read_text(encoding="utf-8"))

    def _backend_set_schema(self, schema: dict[str, Any]) -> None:
        self.schema_path.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _backend_read_item(self, text_sha256: str) -> dict[str, Any] | None:
        path = self.items_dir / f"{text_sha256}.json"
        if not path.exists():
            return None
        return self._read_item_file(path, strict=True)

    def _backend_iter_items(self) -> Iterator[dict[str, Any]]:
        for path in sorted(self.items_dir.glob("*.json")):
            item = self._read_item_file(path, strict=False)
            if item is None:
                continue
            yield item

    def _backend_iter_items_for_work_package(
        self, work_package_id: str
    ) -> Iterator[dict[str, Any]]:
        for item in self._backend_iter_items():
            if item.get("work_package_id") == work_package_id:
                yield item

    def _persist_item(self, item: dict[str, Any]) -> None:
        item_path = self.items_dir / f"{item['text_sha256']}.json"
        self._persist_item_to_disk(item_path, item)

    def _persist_item_to_disk(self, item_path: Path, item: dict[str, Any]) -> None:
        tmp_path = item_path.with_name(f".{item_path.name}.{threading.get_ident()}.tmp")
        tmp_path.write_text(
            json.dumps(item, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(item_path)

    def _read_item_file(self, path: Path, *, strict: bool) -> dict[str, Any] | None:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            if strict:
                raise StorageError(f"Failed to read item file {path.name}: {exc}") from exc
            return None

        if not text.strip():
            if strict:
                raise StorageError(f"Corrupt item file {path.name}: empty file")
            return None

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            if strict:
                raise StorageError(f"Corrupt item file {path.name}: {exc}") from exc
            return None

        if not isinstance(payload, dict):
            if strict:
                raise StorageError(f"Corrupt item file {path.name}: expected object")
            return None
        return payload


class SqliteTextStore(BaseTextStore):
    def __init__(
        self,
        path: str | Path,
        *,
        cache_ttl_seconds: float = 300.0,
        clock: Any | None = None,
    ) -> None:
        self.db_path = Path(path)
        self.db_lock = threading.RLock()
        self.conn: sqlite3.Connection | None = None
        super().__init__(cache_ttl_seconds=cache_ttl_seconds, clock=clock)

    def _init_backend(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            isolation_level=None,
        )
        with self.db_lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS items ("
                "text_sha256 TEXT PRIMARY KEY, "
                "work_package_id TEXT NOT NULL, "
                "payload TEXT NOT NULL"
                ")"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS items_work_package "
                "ON items(work_package_id)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_kv ("
                "id INTEGER PRIMARY KEY CHECK (id = 1), "
                "payload TEXT NOT NULL"
                ")"
            )

    def close(self) -> None:
        with self.db_lock:
            if self.conn is not None:
                self.conn.close()
                self.conn = None

    def _backend_get_schema(self) -> dict[str, Any] | None:
        with self.db_lock:
            row = self.conn.execute(
                "SELECT payload FROM schema_kv WHERE id = 1"
            ).fetchone()
        if not row:
            return None
        return json.loads(row[0])

    def _backend_set_schema(self, schema: dict[str, Any]) -> None:
        payload = json.dumps(schema, sort_keys=True, ensure_ascii=False)
        with self.db_lock:
            self.conn.execute(
                "INSERT INTO schema_kv (id, payload) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
                (payload,),
            )

    def _backend_read_item(self, text_sha256: str) -> dict[str, Any] | None:
        with self.db_lock:
            row = self.conn.execute(
                "SELECT payload FROM items WHERE text_sha256 = ?",
                (text_sha256,),
            ).fetchone()
        if not row:
            return None
        return self._decode_payload(row[0], context=text_sha256)

    def _backend_iter_items(self) -> Iterator[dict[str, Any]]:
        with self.db_lock:
            rows = self.conn.execute(
                "SELECT text_sha256, payload FROM items ORDER BY text_sha256"
            ).fetchall()
        for text_sha256, payload in rows:
            item = self._decode_payload(payload, context=text_sha256, strict=False)
            if item is not None:
                yield item

    def _backend_iter_items_for_work_package(
        self, work_package_id: str
    ) -> Iterator[dict[str, Any]]:
        with self.db_lock:
            rows = self.conn.execute(
                "SELECT text_sha256, payload FROM items "
                "WHERE work_package_id = ? ORDER BY text_sha256",
                (work_package_id,),
            ).fetchall()
        for text_sha256, payload in rows:
            item = self._decode_payload(payload, context=text_sha256, strict=False)
            if item is not None:
                yield item

    def _persist_item(self, item: dict[str, Any]) -> None:
        payload = json.dumps(item, sort_keys=True, ensure_ascii=False)
        with self.db_lock:
            self.conn.execute(
                "INSERT INTO items (text_sha256, work_package_id, payload) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(text_sha256) DO UPDATE SET "
                "  work_package_id = excluded.work_package_id, "
                "  payload = excluded.payload",
                (item["text_sha256"], item["work_package_id"], payload),
            )

    def _decode_payload(
        self, payload: str, *, context: str, strict: bool = True
    ) -> dict[str, Any] | None:
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError as exc:
            if strict:
                raise StorageError(f"Corrupt item payload for {context}: {exc}") from exc
            return None
        if not isinstance(decoded, dict):
            if strict:
                raise StorageError(f"Corrupt item payload for {context}: expected object")
            return None
        return decoded


# Backwards-compatible alias: legacy callers and tests use TextStore for filesystem.
TextStore = FilesystemTextStore


def make_store(
    backend: str,
    path: str | Path,
    *,
    cache_ttl_seconds: float = 300.0,
    clock: Any | None = None,
) -> BaseTextStore:
    normalized = (backend or "filesystem").lower()
    if normalized == "filesystem":
        return FilesystemTextStore(path, cache_ttl_seconds=cache_ttl_seconds, clock=clock)
    if normalized == "sqlite":
        return SqliteTextStore(path, cache_ttl_seconds=cache_ttl_seconds, clock=clock)
    raise StorageError(f"Unsupported storage backend: {backend}")
