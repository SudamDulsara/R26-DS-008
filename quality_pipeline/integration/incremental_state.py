"""
integration/incremental_state.py
--------------------------------
Tracks which upstream documents have already been processed.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS processed_docs (
    source            TEXT NOT NULL,
    upstream_doc_id   TEXT NOT NULL,
    run_id            TEXT NOT NULL,
    processed_at      TEXT NOT NULL,
    PRIMARY KEY (source, upstream_doc_id)
)
"""


class IncrementalState:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_TABLE_DDL)
            conn.commit()

    def already_seen(self, source: str) -> set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT upstream_doc_id FROM processed_docs WHERE source = ?",
                (source,),
            ).fetchall()
        return {r[0] for r in rows}

    def mark_processed(self, entries: Iterable[tuple[str, str]], run_id: str) -> int:
        processed_at = datetime.now(timezone.utc).isoformat()
        entries = list(entries)
        if not entries:
            return 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.executemany(
                "INSERT OR IGNORE INTO processed_docs "
                "(source, upstream_doc_id, run_id, processed_at) "
                "VALUES (?, ?, ?, ?)",
                [(src, uid, run_id, processed_at) for src, uid in entries],
            )
            conn.commit()
            return cursor.rowcount

    def stats(self) -> dict[str, int]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT source, COUNT(*) FROM processed_docs GROUP BY source"
            ).fetchall()
        return {src: n for src, n in rows}


def new_run_id() -> str:
    # Microsecond precision to avoid collisions if runs happen quickly.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
