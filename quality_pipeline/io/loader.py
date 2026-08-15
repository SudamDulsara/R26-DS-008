"""
io/loader.py
------------
Reads raw documents from an upstream source and yields Document objects.

For Phase 0, the "upstream source" is a JSONL file that mimics what the
news/OCR/ASR pipelines will eventually produce. This is deliberate:
we want to develop the quality pipeline independently of the other three
components' completion status.

Later, this module is where a KafkaLoader will live alongside JsonlLoader,
both producing the same Document objects. The rest of the pipeline won't
care which one is used.
"""

import json
from pathlib import Path
from typing import Iterator

from ..schema import Document, Source


class JsonlLoader:
    """
    Reads a JSONL file where each line is one raw document.

    Expected JSON shape per line:
        {
            "doc_id": "news_001",
            "source": "news",           // "news" | "ocr" | "asr"
            "text": "...",              // the actual content
            "metadata": { ... }         // optional, source-specific extras
        }

    Anything extra is tolerated and stored under source_metadata.
    Anything missing falls back to safe defaults.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> Iterator[Document]:
        """Yield Documents one at a time. Generator, so it's memory-friendly."""
        with self.path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue  # skip blank lines

                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    # Fail loudly — a bad input file is a bug, not data to silently skip
                    raise ValueError(
                        f"Invalid JSON at {self.path}:{line_no} — {e}"
                    ) from e

                yield self._to_document(record)

    @staticmethod
    def _to_document(record: dict) -> Document:
        """Convert a raw JSON record into a Document."""
        source_str = record.get("source", "unknown").lower()
        try:
            source = Source(source_str)
        except ValueError:
            source = Source.UNKNOWN  # unknown source types don't crash the pipeline

        text = record.get("text", "")

        return Document(
            doc_id=record.get("doc_id", "missing_id"),
            source=source,
            raw_text=text,
            text=text,  # working copy starts equal to raw
            source_metadata=record.get("metadata", {}),
        )