"""
SQLite Persistence Layer
=========================
Saves every pipeline run so the refined text accumulates into a
permanent dataset instead of disappearing when the Streamlit
session ends.

One row per pipeline run, storing the text at every stage plus the
evaluation scores. This is what makes the "Continuous Dataset
Generation Pipeline" continuous — each processed document becomes
a permanent (raw OCR → corrected) pair that can later be reused as
training data.

Uses Python's built-in sqlite3 — no extra dependency, no server,
one file at data/pipeline_runs.db.

Note: a new connection is opened per call rather than kept at module
level. Streamlit re-runs the script and may use different threads,
and SQLite connections cannot be shared across threads.
"""

import os
import sqlite3
from datetime import datetime

from pipeline.normalize import normalize

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data", "pipeline_runs.db"
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at        TEXT NOT NULL,

    -- where the text came from
    input_mode        TEXT,      -- 'image' or 'text'
    source_filename   TEXT,      -- uploaded image name, if any
    ocr_language      TEXT,      -- Tesseract language actually used

    -- text at each stage of the pipeline
    raw_text          TEXT,      -- Tesseract output / pasted input
    stage1_output     TEXT,      -- after grapheme rules
    stage2_output     TEXT,      -- after AI contextual correction
    final_output      TEXT,      -- after syntactic rules

    -- which correction model produced stage2_output.
    -- 'gemma_api' now; 'byt5_finetuned' once the trained model lands,
    -- so the two can be compared with a GROUP BY.
    correction_method TEXT,

    -- evaluation (NULL when no gold reference was supplied)
    gold_reference    TEXT,
    cer_before        REAL,
    cer_after         REAL,
    wer_before        REAL,
    wer_after         REAL
);
"""


def _connect() -> sqlite3.Connection:
    """Open a connection, creating the data/ directory if needed."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create the table if it does not exist. Safe to call on every startup."""
    with _connect() as conn:
        conn.execute(SCHEMA)


def save_run(
    raw_text: str,
    stage1_output: str,
    stage2_output: str,
    final_output: str,
    correction_method: str,
    input_mode: str,
    source_filename: str = None,
    ocr_language: str = None,
    gold_reference: str = None,
    metrics: dict = None,
) -> int:
    """
    Save one pipeline run.

    `metrics` is the dict returned by pipeline.evaluator.evaluate(),
    or None when the user supplied no gold reference.

    Returns the new row's id.

    Every text field is normalised on the way in, so the accumulated dataset
    is canonical by construction. That matters more here than anywhere else:
    these rows are the training corpus, and a corpus with mixed encodings
    teaches the model that both forms are acceptable output.
    """
    m = metrics or {}

    def _norm(value):
        """Normalise text, passing None through untouched."""
        return normalize(value) if value else value

    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO pipeline_runs (
                created_at, input_mode, source_filename, ocr_language,
                raw_text, stage1_output, stage2_output, final_output,
                correction_method, gold_reference,
                cer_before, cer_after, wer_before, wer_after
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(timespec="seconds"),
                input_mode,
                source_filename,
                ocr_language,
                _norm(raw_text),
                _norm(stage1_output),
                _norm(stage2_output),
                _norm(final_output),
                correction_method,
                _norm(gold_reference) or None,
                m.get("cer_before"),
                m.get("cer_after"),
                m.get("wer_before"),
                m.get("wer_after"),
            ),
        )
        return cur.lastrowid


def fetch_runs(limit: int = None) -> list:
    """Return saved runs, newest first, as a list of dicts."""
    sql = "SELECT * FROM pipeline_runs ORDER BY id DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    with _connect() as conn:
        return [dict(row) for row in conn.execute(sql)]


def count_runs() -> int:
    """Total number of saved runs."""
    with _connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]


def average_metrics_by_method() -> list:
    """
    Average CER/WER grouped by correction method, over runs that have
    a gold reference.

    This is the query behind the thesis results table — once the
    fine-tuned model is added alongside the API, it gives a direct
    before/after comparison per method.
    """
    with _connect() as conn:
        return [dict(row) for row in conn.execute(
            """
            SELECT correction_method,
                   COUNT(*)        AS runs,
                   AVG(cer_before) AS avg_cer_before,
                   AVG(cer_after)  AS avg_cer_after,
                   AVG(wer_before) AS avg_wer_before,
                   AVG(wer_after)  AS avg_wer_after
            FROM pipeline_runs
            WHERE gold_reference IS NOT NULL
            GROUP BY correction_method
            """
        )]
