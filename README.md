# hashharness

Minimal append-only text storage with an MCP server.

## License

This repository is licensed under `CC BY-NC-SA 4.0`.

## Cache

Records are cached in memory by `work_package_id`. A cached work package stays
resident for 5 minutes after its last use, and any access to a record in that
work package refreshes the TTL for the whole package.

Texts are stored as plain JSON files under `data/items/`, so they stay directly grep-able on disk. The storage key is `sha256(text)` (`text_sha256`). Existing items are immutable: the same text hash cannot be rewritten with different metadata or links.

Each stored item also carries integrity hashes:

- `meta_sha256`: hash of the metadata fields `type`, `work_package_id`, `created_at`, `title`, and `attributes`
- `links_sha256`: hash of the validated `links` object
- `record_sha256`: hash of `text_sha256 + meta_sha256 + links_sha256`

**Links between items reference `record_sha256`, not `text_sha256`.** A link therefore pins the target's exact text *and* metadata *and* links — not just its text content. `text_sha256` is the storage key (used to fetch a record by its content); `record_sha256` is the link id (used to refer to a record from another record). The two are distinct: for any item with non-empty metadata, `text_sha256 != record_sha256`.

## Schema

The schema is user-defined and typed. It declares which item types exist and which link fields each type may use.

Example:

```json
{
  "types": {
    "Evidence": {
      "links": {}
    },
    "HypothesisChange": {
      "links": {
        "prevHypothesisChange": {
          "kind": "single",
          "target_types": ["HypothesisChange"]
        },
        "evidences": {
          "kind": "many",
          "target_types": ["Evidence"]
        }
      }
    }
  }
}
```

For every `many` link field, the server also stores a derived `<field>Hash` value based on the sorted list of referenced `record_sha256` values. For `evidences`, that becomes `evidencesHash`.

## Stored Item Shape

```json
{
  "type": "HypothesisChange",
  "text_sha256": "<sha256(text)>",
  "meta_sha256": "<sha256(metadata)>",
  "links_sha256": "<sha256(links)>",
  "record_sha256": "<sha256(text_sha256 + meta_sha256 + links_sha256)>",
  "work_package_id": "wp-123",
  "created_at": "2026-04-25T10:30:00Z",
  "title": "Updated hypothesis",
  "attributes": {
    "author": "alice",
    "status": "draft"
  },
  "text": "....",
  "links": {
    "prevHypothesisChange": "<target record_sha256>",
    "evidences": ["<target record_sha256>", "<target record_sha256>"],
    "evidencesHash": "<sha256(sorted target record_sha256 values)>"
  },
  "stored_at": "2026-04-25T10:00:00+00:00"
}
```

## MCP Tools

The MCP server exposes:

- `set_schema`
- `get_schema`
- `create_item`
- `find_items`
- `get_item_by_hash`
- `get_work_package`
- `find_tip`
- `query_chain`
- `verify_chain`

`find_items` supports substring search by default and regex search when `regex=true`. Searchable fields are `text`, `title`, `work_package_id`, or `all`.
`find_items` also supports optional `fields` projection and exact-match `attributes` filtering.

`create_item` also accepts an optional `attributes` object for arbitrary JSON metadata stored alongside the record and included in `meta_sha256`.
`create_item` accepts `return: "minimal" | "full"`. The default is `minimal`, which returns only `text_sha256` and `record_sha256`.

`get_work_package` returns all records for one exact `work_package_id`, with an optional `type` filter to narrow the result set.

`find_tip` returns the most recent item by `created_at` for one exact `work_package_id` and `type`.

`query_chain` starts from one `text_sha256` (the root), then walks links by `record_sha256` transitively, and returns the full stored records involved in that chain.

`verify_chain` starts from one `text_sha256` (the root), walks links by `record_sha256` transitively, and recomputes `text_sha256`, `meta_sha256`, `links_sha256`, and `record_sha256` for each item. It reports whether the whole chain is intact. With `summary=true`, it returns only `ok`, `checked_items`, `errors_count`, and `root_text_sha256`.

## Run

Start the MCP server over stdio:

```bash
PYTHONPATH=src python3 -m hashharness.mcp_server
```

Optionally choose a storage directory:

```bash
HASHHARNESS_DATA_DIR=/path/to/data PYTHONPATH=src python3 -m hashharness.mcp_server
```

Start the MCP server over HTTP:

```bash
HASHHARNESS_MCP_TRANSPORT=http \
HASHHARNESS_HTTP_HOST=127.0.0.1 \
HASHHARNESS_HTTP_PORT=8000 \
PYTHONPATH=src python3 -m hashharness.mcp_server
```

HTTP transport exposes:

- `POST /mcp` for JSON-RPC/MCP requests
- `GET /health` for a simple health check

Notifications sent to `POST /mcp` are accepted with `202 Accepted`. Requests with an `id` return a normal JSON-RPC response body.

## Test

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```
