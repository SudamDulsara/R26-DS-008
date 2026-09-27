"""
io/training_writer.py
---------------------
Cumulative ACCEPT-tier JSONL corpus, append-mode.
Each record carries the run_id it came from.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from ..schema import Document, Verdict


class TrainingJsonlWriter:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def write(self, docs: Iterable[Document], run_id: str) -> int:
        count = 0
        with self.path.open("a", encoding="utf-8") as f:
            for doc in docs:
                if doc.verdict != Verdict.ACCEPT:
                    continue
                record = self._to_training_record(doc, run_id)
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
        return count

    @staticmethod
    def _to_training_record(doc: Document, run_id: str) -> dict:
        morph = doc.quality.get("morphology", {})
        return {
            "text": doc.text,
            "meta": {
                "doc_id": doc.doc_id,
                "source": doc.source.value,
                "run_id": run_id,
                "morphology_score": morph.get("score"),
                "morphology_features": morph.get("features"),
                "source_metadata": doc.source_metadata,
            },
        }
