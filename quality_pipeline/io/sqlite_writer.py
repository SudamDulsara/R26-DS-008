"""
io/sqlite_writer.py
-------------------
Cumulative 3-table verdicts DB, append-mode, each row tagged with run_id.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable

from ..schema import Document, Verdict


_VERDICT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    doc_id            TEXT PRIMARY KEY,
    source            TEXT NOT NULL,
    text              TEXT NOT NULL,
    raw_text          TEXT,
    verdict           TEXT NOT NULL,
    verdict_reasons   TEXT,
    morphology_score  REAL,
    content_hash      TEXT,
    run_id            TEXT NOT NULL,
    ingested_at       TEXT NOT NULL,
    source_metadata   TEXT
)
"""

_RUN_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS verdict_runs (
    run_id            TEXT PRIMARY KEY,
    started_at        TEXT NOT NULL,
    finished_at       TEXT NOT NULL,
    docs_processed    INTEGER NOT NULL,
    accepted          INTEGER NOT NULL,
    review            INTEGER NOT NULL,
    rejected          INTEGER NOT NULL,
    notes             TEXT
)
"""

_VERDICT_TO_TABLE = {
    Verdict.ACCEPT: "accepted",
    Verdict.REVIEW: "review",
    Verdict.REJECT: "rejected",
}


class SqliteVerdictWriter:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            for table in _VERDICT_TO_TABLE.values():
                conn.execute(_VERDICT_TABLE_DDL.format(table=table))
            conn.execute(_RUN_TABLE_DDL)
            conn.commit()

    def write(
        self,
        docs: Iterable[Document],
        run_id: str,
        started_at: str,
        notes: str = "",
    ) -> dict[str, int]:
        docs = list(docs)
        finished_at = datetime.utcnow().isoformat() + "Z"
        counts = {t: 0 for t in _VERDICT_TO_TABLE.values()}

        with sqlite3.connect(self.path) as conn:
            for doc in docs:
                table = _VERDICT_TO_TABLE.get(doc.verdict)
                if table is None:
                    continue

                morph_score = doc.quality.get("morphology", {}).get("score")
                content_hash = doc.quality.get("content_hash")

                conn.execute(
                    f"INSERT OR REPLACE INTO {table} VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        doc.doc_id,
                        doc.source.value,
                        doc.text,
                        doc.raw_text,
                        doc.verdict.value,
                        "; ".join(doc.verdict_reasons),
                        morph_score,
                        content_hash,
                        run_id,
                        finished_at,
                        json.dumps(doc.source_metadata, ensure_ascii=False),
                    ),
                )
                counts[table] += 1

            conn.execute(
                "INSERT OR REPLACE INTO verdict_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    started_at,
                    finished_at,
                    len(docs),
                    counts["accepted"],
                    counts["review"],
                    counts["rejected"],
                    notes,
                ),
            )
            conn.commit()

        return counts
