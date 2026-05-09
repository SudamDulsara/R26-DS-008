"""
quality/deduplicator.py
-----------------------
Stage 3 (first piece): Exact-hash deduplication.

Catches *exact* duplicates by hashing the cleaned text. Sounds basic, but
news data is full of these — wire-service stories republished by 5 outlets,
the same article scraped twice from different URL paths, archived copies, etc.

Why hash the CLEANED text and not the raw text?
  Two articles that differ only in whitespace, BOMs, or Unicode encoding
  forms are semantically identical. After Stage 1's normalizer they become
  byte-identical, so the hash catches them. Without normalizing first we'd
  miss most syndicated content.

Why exact hash and not MinHash/SimHash for now?
  Exact hashing is O(1) per doc, has zero false positives, and catches the
  bulk of duplicates in news data. Near-duplicate detection (paraphrased
  articles, partial overlap) is a later stage — it's expensive and needs
  threshold tuning we haven't done yet.

This stage is STATEFUL — it remembers hashes across documents in a batch.
That's a meaningful difference from the normalizer (stateless). The base
Stage class doesn't care, but a parallelised orchestrator would have to.
"""

import hashlib

from ..schema import Document, Verdict
from ..stages.base import Stage


class ExactHashDeduplicator(Stage):
    """Reject documents whose cleaned text we've seen before in this batch."""

    name = "quality.exact_hash_dedup"

    def __init__(self) -> None:
        # hash -> doc_id of the first document we saw with that hash
        self._seen: dict[str, str] = {}

    def reset(self) -> None:
        """Clear seen-hashes. Useful between independent batches."""
        self._seen.clear()

    def _process(self, doc: Document) -> Document:
        digest = hashlib.sha256(doc.text.encode("utf-8")).hexdigest()
        doc.quality["content_hash"] = digest

        if digest in self._seen:
            original_id = self._seen[digest]
            doc.verdict = Verdict.REJECT
            doc.verdict_reasons.append(f"duplicate_of:{original_id}")
            doc.quality["is_duplicate"] = True
            doc.quality["duplicate_of"] = original_id
        else:
            self._seen[digest] = doc.doc_id
            doc.quality["is_duplicate"] = False

        return doc