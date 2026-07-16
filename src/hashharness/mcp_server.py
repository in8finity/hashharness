from __future__ import annotations

import json
import os
import sys
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from hashharness.storage import BaseTextStore, StorageError, TextStore, make_store


# Sentinel value returned by _try_acquire_inflight when the cap is reached.
_OVERLOAD = object()


ITEM_FIELD_NAMES = {
    "attributes",
    "created_at",
    "links",
    "links_sha256",
    "meta_sha256",
    "record_sha256",
    "schema_binding_sha256",
    "schema_sha256",
    "text",
    "text_sha256",
    "title",
    "type",
    "work_package_id",
}


class MCPApplication:
    def __init__(self, store: BaseTextStore) -> None:
        self.store = store

    def handle_message(self, request: dict[str, Any]) -> dict[str, Any] | None:
        if "id" not in request:
            self._handle_notification(request)
            return None
        return self._handle_request(request)

    def _handle_notification(self, request: dict[str, Any]) -> None:
        if request.get("method") == "notifications/initialized":
            return

    def _handle_request(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = request["id"]
        method = request.get("method")
        params = request.get("params", {})

        try:
            if method == "initialize":
                return self._result(
                    request_id,
                    {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {
                            "name": "hashharness",
                            "version": "0.1.0",
                        },
                    },
                )
            if method == "ping":
                return self._result(request_id, {})
            if method == "tools/list":
                return self._result(request_id, {"tools": self._tools()})
            if method == "tools/call":
                return self._result(request_id, self._call_tool(params))
        except StorageError as exc:
            return self._tool_error(request_id, str(exc))
        except Exception as exc:  # pragma: no cover - guardrail for protocol handling
            return self._error(request_id, -32000, str(exc))

        return self._error(request_id, -32601, f"Method not found: {method}")

    def _call_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments", {})

        if name == "set_schema":
            result = self.store.set_schema(
                arguments["schema"],
                expected_prev=arguments.get("expected_prev"),
            )
            return self._tool_result(result)
        if name == "get_schema":
            return self._tool_result(self.store.get_schema(at=arguments.get("at")))
        if name == "get_schema_history":
            return self._tool_result({"versions": self.store.get_schema_history()})
        if name == "get_schema_version":
            return self._tool_result(
                self.store.get_schema_version(arguments["record_sha256"])
            )
        if name == "create_item":
            if "created_at" in arguments:
                raise StorageError(
                    "created_at is server-stamped and cannot be supplied by the caller"
                )
            result = self.store.create_item(
                item_type=arguments["type"],
                text=arguments["text"],
                title=arguments["title"],
                work_package_id=arguments["work_package_id"],
                attributes=arguments.get("attributes"),
                links=arguments.get("links", {}),
            )
            return_mode = arguments.get("return", "minimal")
            if return_mode == "minimal":
                return self._tool_result(self._project_item(result, ["text_sha256", "record_sha256"]))
            if return_mode == "full":
                return self._tool_result(result)
            raise StorageError("create_item return must be one of: minimal, full")
        if name == "submit_report_and_finish":
            for section in ("report", "status"):
                spec = arguments.get(section)
                if not isinstance(spec, dict):
                    raise StorageError(
                        f"submit_report_and_finish: `{section}` must be an object"
                    )
                if "created_at" in spec:
                    raise StorageError(
                        f"submit_report_and_finish: `{section}.created_at` is "
                        "server-stamped and cannot be supplied by the caller"
                    )
            result = self.store.submit_report_and_finish(
                work_package_id=arguments["work_package_id"],
                report=arguments["report"],
                status=arguments["status"],
            )
            return_mode = arguments.get("return", "minimal")
            if return_mode == "minimal":
                return self._tool_result({
                    "report": self._project_item(
                        result["report"], ["text_sha256", "record_sha256"]
                    ),
                    "status": self._project_item(
                        result["status"], ["text_sha256", "record_sha256"]
                    ),
                })
            if return_mode == "full":
                return self._tool_result(result)
            raise StorageError(
                "submit_report_and_finish return must be one of: minimal, full"
            )
        if name == "find_items":
            result = self.store.find_items(
                query=arguments.get("query"),
                item_type=arguments.get("type"),
                field=arguments.get("field", "text"),
                regex=bool(arguments.get("regex", False)),
                limit=int(arguments.get("limit", 20)),
                attributes=arguments.get("attributes"),
            )
            fields = arguments.get("fields")
            return self._tool_result({"items": self._project_items(result, fields)})
        if name == "get_item_by_hash":
            result = self.store.get_item(arguments["text_sha256"])
            return self._tool_result(result)
        if name == "get_work_package":
            result = self.store.get_work_package(
                arguments["work_package_id"],
                item_type=arguments.get("type"),
            )
            return self._tool_result(result)
        if name == "find_tip":
            result = self.store.find_tip(
                arguments["work_package_id"],
                arguments["type"],
                where_attributes=arguments.get("where_attributes"),
            )
            fields = arguments.get("fields", ["type", "title", "text_sha256", "record_sha256", "created_at"])
            return self._tool_result(self._project_item(result, fields))
        if name == "find_tips_bulk":
            ids = arguments["work_package_ids"]
            if not isinstance(ids, list):
                raise StorageError("work_package_ids must be a list")
            if len(ids) > 10000:
                raise StorageError("work_package_ids exceeds maximum of 10000 per call")
            tips = self.store.find_tips_bulk(
                ids,
                arguments["type"],
                where_attributes=arguments.get("where_attributes"),
            )
            fields = arguments.get("fields")
            projected: dict[str, dict[str, Any] | None] = {}
            for wp_id, item in tips.items():
                if item is None:
                    projected[wp_id] = None
                elif fields is None:
                    projected[wp_id] = item
                else:
                    projected[wp_id] = self._project_item(item, fields)
            return self._tool_result({"tips": projected})
        if name == "find_tips_where":
            ids = arguments.get("work_package_ids")
            if ids is not None:
                if not isinstance(ids, list):
                    raise StorageError("work_package_ids must be a list")
                if len(ids) > 10000:
                    raise StorageError("work_package_ids exceeds maximum of 10000 per call")
            where = arguments.get("where_attributes")
            if not isinstance(where, dict) or not where:
                raise StorageError("where_attributes must be a non-empty object")
            tips = self.store.find_tips_where(
                arguments["type"], where, work_package_ids=ids
            )
            fields = arguments.get("fields")
            projected: dict[str, dict[str, Any]] = {}
            for wp_id, item in tips.items():
                projected[wp_id] = (
                    item if fields is None else self._project_item(item, fields)
                )
            return self._tool_result({"tips": projected})
        if name == "list_work_packages":
            wps = self.store.list_work_packages(prefix=arguments.get("prefix"))
            return self._tool_result({"work_package_ids": wps})
        if name == "verify_work_package":
            summary = bool(arguments.get("summary", False))
            if "work_package_ids" in arguments:
                ids = arguments["work_package_ids"]
                if not isinstance(ids, list):
                    raise StorageError("work_package_ids must be a list")
                if len(ids) > 10000:
                    raise StorageError("work_package_ids exceeds maximum of 10000 per call")
                results = {
                    wp: self.store.verify_work_package(wp, summary=summary) for wp in ids
                }
                return self._tool_result(
                    {
                        "ok": all(r["ok"] for r in results.values()),
                        "checked_work_packages": len(results),
                        "results": results,
                    }
                )
            result = self.store.verify_work_package(
                arguments["work_package_id"], summary=summary
            )
            return self._tool_result(result)
        if name == "query_chain":
            result = self.store.query_chain(arguments["text_sha256"])
            return self._tool_result(result)
        if name == "verify_chain":
            result = self.store.verify_chain(arguments["text_sha256"])
            if bool(arguments.get("summary", False)):
                return self._tool_result(
                    {
                        "root_text_sha256": result["root_text_sha256"],
                        "ok": result["ok"],
                        "checked_items": result["checked_items"],
                        "errors_count": sum(len(item["errors"]) for item in result["items"]),
                    }
                )
            return self._tool_result(result)

        raise StorageError(f"Unknown tool: {name}")

    def _tools(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "set_schema",
                "description": (
                    "Append a new schema version. Schemas are append-only and "
                    "hash-chained. expected_prev must equal the current schema head "
                    "record_sha256 (or null/omitted for the first/genesis schema); "
                    "stale expected_prev is rejected with 'schema head moved'."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "schema": {"type": "object"},
                        "expected_prev": {
                            "type": ["string", "null"],
                            "description": "record_sha256 of current head, or null for genesis",
                        },
                    },
                    "required": ["schema"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_schema",
                "description": (
                    "Return the schema payload. Without arguments, returns the "
                    "current head's payload; with `at`, returns the payload of "
                    "the schema version with that record_sha256."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "at": {
                            "type": "string",
                            "pattern": "^[0-9a-f]{64}$",
                        },
                    },
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_schema_history",
                "description": (
                    "Return the full schema chain from genesis to current head."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_schema_version",
                "description": (
                    "Return one schema version (full record) by its record_sha256."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "record_sha256": {
                            "type": "string",
                            "pattern": "^[0-9a-f]{64}$",
                        },
                    },
                    "required": ["record_sha256"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "create_item",
                "description": (
                    "Create an immutable text item. Its identifier is sha256(text). "
                    "created_at is server-stamped at write time and cannot be supplied "
                    "by the caller. Link values are the target items' record_sha256 "
                    "(full record hash, binding text + metadata + links), not text_sha256. "
                    "Referenced records must already exist. If the type's schema declares "
                    "a chain_predecessor link, that link's value must equal the current "
                    "head record_sha256 for (work_package_id, type) — or be omitted for "
                    "the first item in the chain. The head advances on each successful "
                    "create; concurrent forks are rejected with 'head moved'."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"},
                        "work_package_id": {"type": "string"},
                        "title": {"type": "string"},
                        "attributes": {"type": "object"},
                        "text": {"type": "string"},
                        "links": {"type": "object"},
                        "return": {
                            "type": "string",
                            "enum": ["minimal", "full"],
                        },
                    },
                    "required": [
                        "type",
                        "work_package_id",
                        "title",
                        "text",
                    ],
                    "additionalProperties": False,
                },
            },
            {
                "name": "submit_report_and_finish",
                "description": (
                    "Append a TaskReport and a terminal TaskStatus in ONE backend "
                    "transaction (one writer-lock acquisition instead of two). Both "
                    "items must belong to the same work_package_id. The server "
                    "injects `proof=<new report.record_sha256>` into "
                    "status.links, so the caller must OMIT it. Both chains' "
                    "chain_predecessor CAS runs under the shared transaction — a "
                    "failure in either rolls both back, so a TaskStatus without "
                    "its proof TaskReport is impossible. Not idempotent: replay "
                    "with the same text sha256 fails loudly."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "work_package_id": {"type": "string"},
                        "report": {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"},
                                "text": {"type": "string"},
                                "attributes": {"type": "object"},
                                "links": {"type": "object"},
                            },
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                        "status": {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"},
                                "text": {"type": "string"},
                                "attributes": {"type": "object"},
                                "links": {"type": "object"},
                            },
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                        "return": {
                            "type": "string",
                            "enum": ["minimal", "full"],
                        },
                    },
                    "required": ["work_package_id", "report", "status"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "find_items",
                "description": (
                    "Search stored items by grep-like substring or regex in text, title, "
                    "work_package_id, or all supported fields."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "type": {"type": "string"},
                        "field": {
                            "type": "string",
                            "enum": ["text", "title", "work_package_id", "all"],
                        },
                        "regex": {"type": "boolean"},
                        "limit": {"type": "integer", "minimum": 1},
                        "fields": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "attributes": {"type": "object"},
                    },
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_item_by_hash",
                "description": "Fetch one item by sha256(text).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "text_sha256": {
                            "type": "string",
                            "pattern": "^[0-9a-f]{64}$",
                        }
                    },
                    "required": ["text_sha256"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_work_package",
                "description": (
                    "Return all records for one work_package_id, optionally filtered by item type."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "work_package_id": {"type": "string"},
                        "type": {"type": "string"},
                    },
                    "required": ["work_package_id"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "find_tip",
                "description": (
                    "Return the tip item for one work package and item type (chain head, "
                    "or most recent by created_at for non-chain types). Optional "
                    "where_attributes requires the tip's attributes to match (exact "
                    "key/value); a non-matching tip is treated as no tip."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "work_package_id": {"type": "string"},
                        "type": {"type": "string"},
                        "fields": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "where_attributes": {
                            "type": "object",
                            "additionalProperties": True,
                        },
                    },
                    "required": ["work_package_id", "type"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "find_tips_bulk",
                "description": (
                    "Return the tip item for each given work_package_id and item type "
                    "in a single call. Result is a dict keyed by work_package_id; "
                    "missing chains map to null (no error). Up to 10000 ids per call. "
                    "Use this instead of N find_tip calls when rendering dashboards or "
                    "summary views over many chains. Optional where_attributes filters "
                    "to tips whose attributes match exactly (e.g. {\"status\": \"new\"}); "
                    "non-matching tips map to null."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "work_package_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 10000,
                        },
                        "type": {"type": "string"},
                        "fields": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "where_attributes": {
                            "type": "object",
                            "additionalProperties": True,
                        },
                    },
                    "required": ["work_package_ids", "type"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "find_tips_where",
                "description": (
                    "Return current chain tips whose attributes match where_attributes "
                    "(exact key/value), keyed by work_package_id. Unlike find_tips_bulk "
                    "you need not enumerate candidate work packages: the sqlite backend "
                    "answers in O(matching tips) via a maintained tip-attribute index, so "
                    "'all TaskStatus tips with status=new' stays cheap as history grows. "
                    "Optional work_package_ids restricts the result to that set."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"},
                        "where_attributes": {
                            "type": "object",
                            "additionalProperties": True,
                            "minProperties": 1,
                        },
                        "work_package_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 10000,
                        },
                        "fields": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["type", "where_attributes"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "list_work_packages",
                "description": (
                    "Return every distinct work_package_id (optionally restricted to those "
                    "starting with `prefix`), sorted. Enumeration primitive for auditing the "
                    "store without prior knowledge of what it contains; pair with "
                    "verify_work_package for a whole-store or scoped integrity sweep."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "prefix": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            },
            {
                "name": "verify_work_package",
                "description": (
                    "Verify EVERY record stored in a work package (or a list of them via "
                    "work_package_ids), not just those reachable from a root — so an orphan "
                    "or unlinked record is caught, and each record's bound schema is "
                    "re-checked against the canonical schema chain. Does not follow links "
                    "into other work packages. With summary=true returns counts only. "
                    "Compose with list_work_packages for a whole-store sweep."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "work_package_id": {"type": "string"},
                        "work_package_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 10000,
                        },
                        "summary": {"type": "boolean"},
                    },
                    "additionalProperties": False,
                },
            },
            {
                "name": "verify_chain",
                "description": (
                    "Verify one item and all transitively linked items by recomputing text, "
                    "metadata, links, and combined record hashes."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "text_sha256": {
                            "type": "string",
                            "pattern": "^[0-9a-f]{64}$",
                        },
                        "summary": {"type": "boolean"},
                    },
                    "required": ["text_sha256"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "query_chain",
                "description": (
                    "Return one item and all transitively linked items starting from a "
                    "root text sha256."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "text_sha256": {
                            "type": "string",
                            "pattern": "^[0-9a-f]{64}$",
                        }
                    },
                    "required": ["text_sha256"],
                    "additionalProperties": False,
                },
            },
        ]

    def _tool_result(self, payload: Any) -> dict[str, Any]:
        return {
            "content": [{"type": "text", "text": json.dumps(payload, indent=2, sort_keys=True)}],
            "structuredContent": payload,
        }

    def _project_items(
        self, items: list[dict[str, Any]], fields: list[str] | None
    ) -> list[dict[str, Any]]:
        if fields is None:
            return items
        return [self._project_item(item, fields) for item in items]

    def _project_item(self, item: dict[str, Any], fields: list[str]) -> dict[str, Any]:
        unknown = [field for field in fields if field not in ITEM_FIELD_NAMES]
        if unknown:
            raise StorageError(f"Unknown item fields requested: {', '.join(sorted(unknown))}")
        return {field: item.get(field) for field in fields if field in item}

    def _tool_error(self, request_id: Any, message: str) -> dict[str, Any]:
        return self._result(
            request_id,
            {
                "content": [{"type": "text", "text": message}],
                "isError": True,
            },
        )

    def _result(self, request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def _error(self, request_id: Any, code: int, message: str) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }


class StdioMCPServer:
    def __init__(self, app: MCPApplication) -> None:
        self.app = app

    def run(self) -> None:
        while True:
            request = self._read_message()
            if request is None:
                return
            response = self.app.handle_message(request)
            if response is not None:
                self._write_message(response)

    def _read_message(self) -> dict[str, Any] | None:
        headers: dict[str, str] = {}
        while True:
            line = sys.stdin.buffer.readline()
            if not line:
                return None
            if line in {b"\r\n", b"\n"}:
                break
            key, _, value = line.decode("utf-8").partition(":")
            headers[key.strip().lower()] = value.strip()

        content_length = int(headers["content-length"])
        body = sys.stdin.buffer.read(content_length)
        return json.loads(body.decode("utf-8"))

    def _write_message(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        header = f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8")
        sys.stdout.buffer.write(header)
        sys.stdout.buffer.write(body)
        sys.stdout.buffer.flush()


class HttpMCPServer:
    def __init__(
        self,
        app: MCPApplication,
        host: str,
        port: int,
        *,
        max_inflight: int = 64,
        retry_after_seconds: int = 1,
        listen_backlog: int = 128,
    ) -> None:
        self.app = app
        self.host = host
        self.port = port
        # 0 disables the inflight cap (clients see no 503 backpressure).
        # Otherwise BoundedSemaphore enforces it across all request threads.
        self.max_inflight = max(0, int(max_inflight))
        self.retry_after_seconds = max(0, int(retry_after_seconds))
        self.listen_backlog = max(1, int(listen_backlog))
        self._inflight = (
            threading.BoundedSemaphore(self.max_inflight)
            if self.max_inflight > 0
            else None
        )

    def handle_http_request(
        self,
        *,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
    ) -> tuple[HTTPStatus, dict[str, str], bytes]:
        headers = headers or {}

        if method == "GET":
            if path == "/health":
                # Always cheap; never backpressured — useful for liveness
                # checks during overload.
                return self._json_response(HTTPStatus.OK, {"ok": True})
            return self._json_response(
                HTTPStatus.METHOD_NOT_ALLOWED,
                {"error": "Use POST /mcp for MCP requests"},
            )

        if method != "POST":
            return self._json_response(
                HTTPStatus.METHOD_NOT_ALLOWED,
                {"error": f"Unsupported method: {method}"},
            )

        if path != "/mcp":
            return self._json_response(
                HTTPStatus.NOT_FOUND,
                {"error": "Not found", "path": path},
            )

        if body:
            try:
                request = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError:
                return self._json_response(
                    HTTPStatus.BAD_REQUEST,
                    {"error": "Request body must be valid JSON"},
                )
        else:
            return self._json_response(
                HTTPStatus.BAD_REQUEST,
                {"error": "Request body must be valid JSON"},
            )

        # Inflight backpressure: if every slot is taken, return 503 +
        # Retry-After so the client backs off cleanly instead of piling
        # another concurrent request onto a contended writer (which the
        # old behaviour surfaced as ConnectionResetError [Errno 54]).
        token = self._try_acquire_inflight()
        if token is _OVERLOAD:
            return (
                HTTPStatus.SERVICE_UNAVAILABLE,
                {
                    "Content-Type": "application/json",
                    "Retry-After": str(self.retry_after_seconds),
                },
                json.dumps(
                    {
                        "error": "server overloaded",
                        "retry_after_seconds": self.retry_after_seconds,
                    }
                ).encode("utf-8"),
            )
        try:
            response = self.app.handle_message(request)
        finally:
            self._release_inflight(token)

        if response is None:
            return HTTPStatus.ACCEPTED, {"Content-Length": "0"}, b""
        return self._json_response(HTTPStatus.OK, response)

    def _try_acquire_inflight(self) -> object:
        if self._inflight is None:
            return None
        if self._inflight.acquire(blocking=False):
            return self._inflight
        return _OVERLOAD

    def _release_inflight(self, token: object) -> None:
        if isinstance(token, threading.BoundedSemaphore):
            token.release()

    def serve_forever(self) -> None:
        http_server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "hashharness/0.1.0"
            protocol_version = "HTTP/1.1"

            def do_POST(self) -> None:  # noqa: N802
                # HTTP-level guard so any unhandled error returns a JSON 500
                # to the client rather than aborting mid-stream and tearing
                # the connection (which the client would see as Errno 54 /
                # ConnectionResetError, indistinguishable from a real crash).
                try:
                    try:
                        content_length = int(self.headers.get("Content-Length", "0"))
                    except ValueError:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {"error": "Invalid Content-Length header"},
                        )
                        return

                    body = self.rfile.read(content_length)
                    status, response_headers, response_body = http_server.handle_http_request(
                        method="POST",
                        path=self.path,
                        headers={key: value for key, value in self.headers.items()},
                        body=body,
                    )
                    self._send_response(status, response_headers, response_body)
                except Exception as exc:  # pragma: no cover - last-ditch guardrail
                    try:
                        self._send_json(
                            HTTPStatus.INTERNAL_SERVER_ERROR,
                            {"error": "internal server error", "detail": str(exc)},
                        )
                    except Exception:
                        pass

            def do_GET(self) -> None:  # noqa: N802
                status, response_headers, response_body = http_server.handle_http_request(
                    method="GET",
                    path=self.path,
                    headers={key: value for key, value in self.headers.items()},
                )
                self._send_response(status, response_headers, response_body)

            def log_message(self, format: str, *args: Any) -> None:
                return

            def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
                response_status, response_headers, response_body = http_server._json_response(
                    status,
                    payload,
                )
                self._send_response(response_status, response_headers, response_body)

            def _send_response(
                self,
                status: HTTPStatus,
                response_headers: dict[str, str],
                response_body: bytes,
            ) -> None:
                self.send_response(status)
                for key, value in response_headers.items():
                    self.send_header(key, value)
                self.end_headers()
                if response_body:
                    self.wfile.write(response_body)

        # request_queue_size is the listen(backlog) socket parameter — the
        # OS-level accept queue. The stdlib default is 5; under a 6-worker
        # burst that produces TCP RST (ECONNRESET / Errno 54) on whichever
        # connects don't fit. Configurable; default 128.
        ThreadingHTTPServer.request_queue_size = self.listen_backlog
        with ThreadingHTTPServer((self.host, self.port), Handler) as server:
            server.serve_forever()

    def _json_response(
        self, status: HTTPStatus, payload: dict[str, Any]
    ) -> tuple[HTTPStatus, dict[str, str], bytes]:
        body = json.dumps(payload).encode("utf-8")
        return (
            status,
            {
                "Content-Type": "application/json",
                "Content-Length": str(len(body)),
            },
            body,
        )


def main() -> None:
    backend = os.environ.get("HASHHARNESS_STORAGE_BACKEND", "filesystem")
    default_path = "data" if backend == "filesystem" else "data/hashharness.sqlite"
    data_path = Path(os.environ.get("HASHHARNESS_DATA_DIR", default_path))
    store = make_store(
        backend,
        data_path,
        wal_autocheckpoint_pages=int(
            os.environ.get("HASHHARNESS_WAL_AUTOCHECKPOINT_PAGES", "1000")
        ),
        wal_checkpoint_writes=int(
            os.environ.get("HASHHARNESS_WAL_CHECKPOINT_WRITES", "1000")
        ),
    )
    app = MCPApplication(store)

    transport = os.environ.get("HASHHARNESS_MCP_TRANSPORT", "stdio")
    if transport == "stdio":
        StdioMCPServer(app).run()
        return
    if transport == "http":
        host = os.environ.get("HASHHARNESS_HTTP_HOST", "127.0.0.1")
        port = int(os.environ.get("HASHHARNESS_HTTP_PORT", "8000"))
        HttpMCPServer(
            app,
            host,
            port,
            max_inflight=int(os.environ.get("HASHHARNESS_MAX_INFLIGHT", "64")),
            retry_after_seconds=int(
                os.environ.get("HASHHARNESS_RETRY_AFTER_SECONDS", "1")
            ),
            listen_backlog=int(os.environ.get("HASHHARNESS_HTTP_BACKLOG", "128")),
        ).serve_forever()
        return
    raise SystemExit(f"Unsupported HASHHARNESS_MCP_TRANSPORT: {transport}")


if __name__ == "__main__":
    main()
