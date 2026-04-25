"""Convert a filesystem-backed hashharness store to a SQLite-backed one.

Usage:
    python -m hashharness.migrate --src data --dst data/hashharness.sqlite
    python -m hashharness.migrate --src data --dst data/hashharness.sqlite --verify
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hashharness.storage import (
    FilesystemTextStore,
    SqliteTextStore,
    StorageError,
)


def migrate(
    src: Path,
    dst: Path,
    *,
    verify: bool = False,
    overwrite: bool = False,
) -> dict[str, int]:
    if not src.exists():
        raise StorageError(f"Source directory does not exist: {src}")
    if dst.exists() and not overwrite:
        raise StorageError(
            f"Destination already exists: {dst}. Pass --overwrite to replace it."
        )
    if dst.exists() and overwrite:
        dst.unlink()

    fs_store = FilesystemTextStore(src)
    sqlite_store = SqliteTextStore(dst)

    try:
        schema = fs_store.get_schema()
        if schema.get("types"):
            sqlite_store.set_schema(schema)

        item_count = 0
        skipped = 0
        for item in fs_store._backend_iter_items():
            sqlite_store._persist_item(item)
            item_count += 1

        # Count corrupt/empty files for reporting.
        for path in fs_store.items_dir.glob("*.json"):
            if fs_store._read_item_file(path, strict=False) is None:
                skipped += 1

        if verify:
            for item in fs_store._backend_iter_items():
                report = sqlite_store.verify_chain(item["text_sha256"])
                if not report["ok"]:
                    raise StorageError(
                        f"Verification failed for {item['text_sha256']}: "
                        f"{report['items']}"
                    )

        return {"items_copied": item_count, "skipped_files": skipped}
    finally:
        fs_store.flush_writes()
        sqlite_store.flush_writes()
        sqlite_store.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--src",
        type=Path,
        required=True,
        help="Filesystem store root (contains items/ and schema.json).",
    )
    parser.add_argument(
        "--dst",
        type=Path,
        required=True,
        help="Path to the SQLite database file to create.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Run verify_chain on every migrated item after writing.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace the destination file if it already exists.",
    )
    args = parser.parse_args(argv)

    try:
        result = migrate(
            args.src,
            args.dst,
            verify=args.verify,
            overwrite=args.overwrite,
        )
    except StorageError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Migrated {result['items_copied']} item(s) from {args.src} -> {args.dst}"
        + (f" (skipped {result['skipped_files']} corrupt/empty file(s))"
           if result["skipped_files"]
           else "")
    )
    if args.verify:
        print("Verification: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
