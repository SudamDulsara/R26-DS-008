"""
Storage for Pipeline B — SQLite for state, JSONL for the deliverable
====================================================================

Two files, two jobs:

    data/ocr_correction_pairs.db   the database. Every generated pair plus
                                   the bookkeeping: which pages are done,
                                   what each run did, what got flagged.
    data/generated/pairs.jsonl     the same pairs as a flat file, for people
                                   who want to download the dataset rather
                                   than query it.

LAYOUT IS FIXED BY THE GROUP, NOT BY THIS MODULE
------------------------------------------------
The group agreed: SQLite, the database file directly inside a `data` folder
at the repository root, and any supporting files in a subfolder beneath it.
That is why the .db sits at data/ and the JSONL at data/generated/ rather
than both living together. Do not "tidy" them back into one folder.

Why both, rather than one:

  - The JSONL is what a researcher downloads. Nobody wants to be handed a
    SQLite file and told to write SQL to read a dataset.
  - The database is what makes the pipeline *resumable*. Asking "have I
    already done page 7 of this document?" against a growing JSONL means
    re-reading the whole file; against an indexed table it is instant.

The group's shared database is a separate, later thing. When the other two
members agree a schema, this exports into it. Blocking Pipeline B on that
agreement would have made progress depend on two other people.

RESUMPTION IS THE POINT
-----------------------
"Continuous" means the pipeline can be killed at any moment and pick up
exactly where it stopped, without redoing work and without duplicating rows.
That property lives here, in the UNIQUE(doc_id, page_num) constraint and in
`already_done()`.

A page is identified by the CONTENT of its source file, not its name. Renaming
a PDF must not cause it to be processed twice, and replacing a file's contents
while keeping the name must not let stale results stand.
"""

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: The database goes directly in data/ ; everything else in a subfolder.
#: Group convention -- see the module docstring.
DEFAULT_DATA = os.path.join(HERE, "data")
DEFAULT_DB_NAME = "ocr_correction_pairs.db"
DEFAULT_SUPPORT = os.path.join(DEFAULT_DATA, "generated")

SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,

    -- identity. doc_id is a hash of the source file's bytes, so the same
    -- document under a different filename is still the same document.
    doc_id          TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    page_num        INTEGER NOT NULL,

    created_at      TEXT NOT NULL,
    run_id          TEXT NOT NULL,

    -- the pair itself
    raw_text        TEXT,           -- Tesseract. The "wrong" side.
    corrected_text  TEXT,           -- model output. MACHINE-CORRECTED, NOT
                                    -- VERIFIED. There is no gold text for a
                                    -- scanned Act; this is not ground truth.
    corrector       TEXT,           -- which model produced corrected_text

    -- quality signals. No gold text exists on production input, so these are
    -- proxies, not accuracy. They exist so bad rows can be found later.
    raw_chars       INTEGER,
    corrected_chars INTEGER,
    length_ratio    REAL,           -- corrected/raw; far from 1.0 is suspect
    flags           TEXT,           -- comma-separated; '' means clean

    ocr_seconds     REAL,
    correct_seconds REAL,

    UNIQUE(doc_id, page_num)
);

CREATE INDEX IF NOT EXISTS idx_pages_doc ON pages(doc_id);
CREATE INDEX IF NOT EXISTS idx_pages_flags ON pages(flags);

CREATE TABLE IF NOT EXISTS runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    input_dir       TEXT,
    corrector       TEXT,
    pages_written   INTEGER DEFAULT 0,
    pages_skipped   INTEGER DEFAULT 0,   -- already done on an earlier run
    pages_flagged   INTEGER DEFAULT 0,
    documents_seen  INTEGER DEFAULT 0,
    note            TEXT
);

CREATE TABLE IF NOT EXISTS documents (
    doc_id          TEXT PRIMARY KEY,
    source_file     TEXT,
    page_count      INTEGER,
    first_seen      TEXT,
    bytes           INTEGER,
    kind            TEXT,       -- 'scanned' | 'digital' | 'unclear'
    chars_per_page  REAL,       -- extractable text; ~0 means a real scan
    images_per_page REAL,
    accepted        INTEGER     -- 0 = rejected, with reason in `note`
);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def file_digest(path: str) -> str:
    """
    Short content hash, used as doc_id.

    Read in chunks: an Act can be tens of megabytes and there is no reason to
    hold one in memory just to hash it.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


class Store:
    """
    Owns both output files.

    A connection is opened per operation rather than held. The pipeline is a
    long-running process that may be killed at any moment, and a connection
    left open across a crash can leave the database locked. Per-call
    connections cost microseconds and remove that failure mode entirely.
    """

    def __init__(self, data_dir: str = DEFAULT_DATA,
                 db_name: str = DEFAULT_DB_NAME,
                 support_dir: str = DEFAULT_SUPPORT):
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(support_dir, exist_ok=True)
        self.db_path = os.path.join(data_dir, db_name)
        self.jsonl_path = os.path.join(support_dir, "pairs.jsonl")
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    # ── resumption ──────────────────────────────────────────────

    def already_done(self, doc_id: str) -> set:
        """Page numbers already stored for this document."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT page_num FROM pages WHERE doc_id = ?", (doc_id,)
            )
            return {r["page_num"] for r in rows}

    def register_document(self, doc_id, source_file, page_count, size_bytes,
                          kind="unknown", chars_per_page=None,
                          images_per_page=None, accepted=1):
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO documents "
                "(doc_id, source_file, page_count, first_seen, bytes, "
                " kind, chars_per_page, images_per_page, accepted) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (doc_id, source_file, page_count, now(), size_bytes,
                 kind, chars_per_page, images_per_page, int(accepted)),
            )

    # ── runs ────────────────────────────────────────────────────

    def start_run(self, run_id, input_dir, corrector, note=None):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, started_at, input_dir, corrector, note) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, now(), input_dir, corrector, note),
            )

    def finish_run(self, run_id, written, skipped, flagged, documents):
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET finished_at = ?, pages_written = ?, "
                "pages_skipped = ?, pages_flagged = ?, documents_seen = ? "
                "WHERE run_id = ?",
                (now(), written, skipped, flagged, documents, run_id),
            )

    # ── writing a pair ──────────────────────────────────────────

    def save_pair(self, record: dict) -> bool:
        """
        Write one page to both files.

        The database write happens FIRST and its UNIQUE constraint is the
        gatekeeper. If it rejects the row as a duplicate, nothing is appended
        to the JSONL. Doing it the other way round would let a crash between
        the two writes duplicate a line in the published dataset, which is far
        worse than losing one — a duplicate is invisible and permanent.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO pages (
                    doc_id, source_file, page_num, created_at, run_id,
                    raw_text, corrected_text, corrector,
                    raw_chars, corrected_chars, length_ratio, flags,
                    ocr_seconds, correct_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["doc_id"], record["source_file"], record["page_num"],
                    now(), record["run_id"],
                    record["raw_text"], record["corrected_text"],
                    record["corrector"],
                    record["raw_chars"], record["corrected_chars"],
                    record["length_ratio"], ",".join(record["flags"]),
                    record.get("ocr_seconds"), record.get("correct_seconds"),
                ),
            )
            if cur.rowcount == 0:
                return False        # already present; do not append again

        published = {
            "doc_id": record["doc_id"],
            "source_file": record["source_file"],
            "page_num": record["page_num"],
            "raw": record["raw_text"],
            "corrected": record["corrected_text"],
            "corrector": record["corrector"],
            "flags": record["flags"],
            # Stated on every row, not just in the README. Anyone who reads a
            # single line of this file should know what the column is.
            "corrected_is": "machine-generated, not human-verified",
        }
        with open(self.jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(published, ensure_ascii=False) + "\n")
        return True

    # ── reporting ───────────────────────────────────────────────

    def summary(self) -> dict:
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) c FROM pages").fetchone()["c"]
            docs = conn.execute("SELECT COUNT(*) c FROM documents").fetchone()["c"]
            flagged = conn.execute(
                "SELECT COUNT(*) c FROM pages WHERE flags != ''"
            ).fetchone()["c"]
            by_flag = {}
            for row in conn.execute(
                "SELECT flags, COUNT(*) c FROM pages WHERE flags != '' GROUP BY flags"
            ):
                by_flag[row["flags"]] = row["c"]
            runs = conn.execute("SELECT COUNT(*) c FROM runs").fetchone()["c"]
            by_kind = {}
            for row in conn.execute(
                "SELECT kind, accepted, COUNT(*) c FROM documents "
                "GROUP BY kind, accepted"
            ):
                label = row["kind"] + ("" if row["accepted"] else " (rejected)")
                by_kind[label] = row["c"]
        return {
            "pages": total, "documents": docs, "flagged": flagged,
            "by_flag": by_flag, "by_kind": by_kind, "runs": runs,
            "jsonl": self.jsonl_path, "db": self.db_path,
        }
