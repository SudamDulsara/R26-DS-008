"""
io/sqlite_loader.py
-------------------
Reads documents directly from the three upstream SQLite databases.

  news_pipeline.db           — table: articles       (Component 1)
  ocr_correction_pairs.db    — table: pages         (Component 2)
  videos.db                  — table: clips         (Component 3)

Each schema has different column names, so each source has its own adapter.
"""

from __future__ import annotations

import logging
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

from ..schema import Document, Source

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base adapter — every source implements this
# ---------------------------------------------------------------------------

class SqliteSourceAdapter(ABC):
    """Adapter for one upstream SQLite database."""

    source_enum: Source
    id_prefix: str

    @abstractmethod
    def query(self) -> str:
        ...

    @abstractmethod
    def row_to_doc(self, row: sqlite3.Row) -> Document | None:
        ...

    def load(self, db_path: Path) -> Iterator[Document]:
        if not db_path.exists():
            log.warning("%s DB not found at %s — skipping source", self.source_enum.value, db_path)
            return

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            try:
                cursor = conn.execute(self.query())
            except sqlite3.OperationalError as e:
                # Missing table / bad SQL — log and skip rather than crash the batch.
                log.warning(
                    "%s adapter query failed on %s: %s. Skipping this source.",
                    self.source_enum.value, db_path, e,
                )
                return
            for row in cursor:
                try:
                    doc = self.row_to_doc(row)
                    if doc is not None:
                        yield doc
                except Exception as e:
                    log.warning(
                        "Skipping malformed %s row: %s (row keys: %s)",
                        self.source_enum.value, e, list(row.keys()) if row else "?",
                    )
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# News adapter (Component 1)
# ---------------------------------------------------------------------------
# Actual schema (news_pipeline.db):
#   Table: articles
#   Columns include: id, url, source, title, title_source, author,
#                    author_source, ...and a text-body column
#
# CONFIG: set NEWS_TEXT_COLUMN below to whichever column holds the article
# body. Common names: 'content', 'body', 'text', 'article', 'article_text'.
# If unsure, run in SQLite Browser:
#     SELECT name FROM pragma_table_info('articles');
# and update the constant below to the correct name.
NEWS_TEXT_COLUMN = "story"


class NewsAdapter(SqliteSourceAdapter):
    source_enum = Source.NEWS
    id_prefix = "news"

    def query(self) -> str:
        return f"""
            SELECT story_id, cluster_id, title, last_updated,
                {NEWS_TEXT_COLUMN} AS body
            FROM final_unified_stories
            WHERE {NEWS_TEXT_COLUMN} IS NOT NULL AND TRIM({NEWS_TEXT_COLUMN}) != ''
        """
    def row_to_doc(self, row: sqlite3.Row) -> Document | None:
        body = (row["body"] or "").strip()
        if not body:
            return None
        return Document(
            doc_id=f"{self.id_prefix}_{row['story_id']}",
            source=self.source_enum,
            raw_text=body,
            text=body,
            source_metadata={
                "story_id": row["story_id"],
                "cluster_id": row["cluster_id"],
                "title": row["title"],
                "last_updated": row["last_updated"],
            },
        )

# ---------------------------------------------------------------------------
# OCR adapter (Component 2)
# ---------------------------------------------------------------------------
# Actual schema (ocr_correction_pairs.db):
#   Table: pages
#   Columns: id, doc_id, source_file, page_num, created_at, run_id, raw_text
#
# Note: `raw_text` here means "text extracted from the OCR page" — despite
# the column name, this IS the deliverable text for our pipeline. There's
# no separate cleaned/corrected column in this schema.

class OcrAdapter(SqliteSourceAdapter):
    source_enum = Source.OCR
    id_prefix = "ocr"

    def query(self) -> str:
        return """
            SELECT id, doc_id, source_file, page_num, created_at, run_id, raw_text
            FROM pages
            WHERE raw_text IS NOT NULL AND TRIM(raw_text) != ''
        """

    def row_to_doc(self, row: sqlite3.Row) -> Document | None:
        text = (row["raw_text"] or "").strip()
        if not text:
            return None
        return Document(
            doc_id=f"{self.id_prefix}_{row['id']}",
            source=self.source_enum,
            raw_text=text,
            text=text,
            source_metadata={
                "upstream_doc_id": row["doc_id"],
                "source_file": row["source_file"],
                "page_num": row["page_num"],
                "created_at": row["created_at"],
                "upstream_run_id": row["run_id"],
            },
        )


# ---------------------------------------------------------------------------
# ASR adapter (Component 3)
# ---------------------------------------------------------------------------
# Actual schema (videos.db):
#   Table: clips
#   Columns: clip_id, video_id, clip_name, start_time, end_time, duration,
#            transcript, verified, language, drive_file_id, created_at

class AsrAdapter(SqliteSourceAdapter):
    source_enum = Source.ASR
    id_prefix = "asr"

    def query(self) -> str:
        return """
            SELECT clip_id, video_id, clip_name, start_time, end_time, duration,
                   transcript, verified, language
            FROM clips
            WHERE transcript IS NOT NULL AND TRIM(transcript) != ''
        """

    def row_to_doc(self, row: sqlite3.Row) -> Document | None:
        text = (row["transcript"] or "").strip()
        if not text:
            return None
        return Document(
            doc_id=f"{self.id_prefix}_{row['clip_id']}",
            source=self.source_enum,
            raw_text=text,
            text=text,
            source_metadata={
                "clip_name": row["clip_name"],
                "video_id": row["video_id"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "duration": row["duration"],
                "upstream_verified": bool(row["verified"]),
                "language": row["language"],
            },
        )


# ---------------------------------------------------------------------------
# The main loader
# ---------------------------------------------------------------------------

class MultiSqliteLoader:
    """Reads Documents from all three upstream SQLite databases."""

    DEFAULT_DB_FILENAMES = {
        Source.NEWS: "news_pipeline.db",
        Source.OCR:  "ocr_correction_pairs.db",
        Source.ASR:  "videos.db",
    }

    ADAPTERS: dict[Source, type[SqliteSourceAdapter]] = {
        Source.NEWS: NewsAdapter,
        Source.OCR:  OcrAdapter,
        Source.ASR:  AsrAdapter,
    }

    def __init__(
        self,
        inbox_dir: str | Path,
        db_filenames: dict[Source, str] | None = None,
    ):
        self.inbox_dir = Path(inbox_dir)
        self.db_filenames = db_filenames or self.DEFAULT_DB_FILENAMES

    def load(
        self,
        skip_seen: dict[Source, set[str]] | None = None,
    ) -> Iterator[Document]:
        skip_seen = skip_seen or {}
        for source, filename in self.db_filenames.items():
            already = skip_seen.get(source, set())
            adapter_cls = self.ADAPTERS[source]
            adapter = adapter_cls()
            db_path = self.inbox_dir / filename
            for doc in adapter.load(db_path):
                prefix = f"{adapter.id_prefix}_"
                upstream_id = doc.doc_id[len(prefix):] if doc.doc_id.startswith(prefix) else doc.doc_id
                if upstream_id in already:
                    continue
                yield doc

    def count_available(self) -> dict[Source, int]:
        counts: dict[Source, int] = {}
        for source, filename in self.db_filenames.items():
            db_path = self.inbox_dir / filename
            if not db_path.exists():
                counts[source] = -1
                continue
            adapter = self.ADAPTERS[source]()
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(adapter.query())
                counts[source] = sum(1 for _ in cursor)
            except sqlite3.Error as e:
                log.warning("Failed to count rows in %s: %s", db_path, e)
                counts[source] = -1
            finally:
                conn.close()
        return counts