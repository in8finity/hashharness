from __future__ import annotations

import json
import os
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from hashharness.storage import BaseTextStore, StorageError, TextStore, make_store


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
            )
            fields = arguments.get("fields", ["type", "title", "text_sha256", "record_sha256", "created_at"])
            return self._tool_result(self._project_item(result, fields))
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
                    "Return the most recent item by created_at for one work package and item type."
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
                    },
                    "required": ["work_package_id", "type"],
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
    def __init__(self, app: MCPApplication, host: str, port: int) -> None:
        self.app = app
        self.host = host
        self.port = port

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

        response = self.app.handle_message(request)
        if response is None:
            return HTTPStatus.ACCEPTED, {"Content-Length": "0"}, b""
        return self._json_response(HTTPStatus.OK, response)

    def serve_forever(self) -> None:
        http_server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "hashharness/0.1.0"
            protocol_version = "HTTP/1.1"

            def do_POST(self) -> None:  # noqa: N802
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
    store = make_store(backend, data_path)
    app = MCPApplication(store)

    transport = os.environ.get("HASHHARNESS_MCP_TRANSPORT", "stdio")
    if transport == "stdio":
        StdioMCPServer(app).run()
        return
    if transport == "http":
        host = os.environ.get("HASHHARNESS_HTTP_HOST", "127.0.0.1")
        port = int(os.environ.get("HASHHARNESS_HTTP_PORT", "8000"))
        HttpMCPServer(app, host, port).serve_forever()
        return
    raise SystemExit(f"Unsupported HASHHARNESS_MCP_TRANSPORT: {transport}")


if __name__ == "__main__":
    main()
