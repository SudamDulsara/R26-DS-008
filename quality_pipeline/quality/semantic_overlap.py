"""
quality/semantic_overlap.py
---------------------------
Stage 3b: Cross-register semantic overlap detection. (Novelty 2)

What this stage does
--------------------
For each document, produce a sentence-embedding vector using a multilingual
sentence-transformer (LaBSE by default), compare it against every previously-
seen document's vector using cosine similarity, and apply register-aware
verdict logic:

  - Similarity > threshold AND same source  → REJECT (near-duplicate)
  - Similarity > threshold AND different source → REVIEW (cross-register overlap)
  - Similarity ≤ threshold                    → no change (keep current verdict)

Why this is the second novelty
------------------------------
Stage 2 (exact-hash dedup) catches only byte-identical content after
normalization. It cannot catch paraphrases: "the prime minister announced X"
vs "today's announcement from the PM was X" produce completely different
hashes. This stage catches them.

Beyond generic semantic dedup, this stage is REGISTER-AWARE — it uses the
`source` field on each Document (news / ocr / asr) to distinguish two very
different situations:

  1. Two news articles saying the same thing → syndicated content, reject.
     The corpus doesn't need both copies.

  2. A news article and an ASR transcript covering the same event → same
     content in different registers (formal written vs conversational spoken).
     This is potentially VALUABLE training data — a language model benefits
     from seeing both renderings — so route to REVIEW, not REJECT.

Existing near-duplicate detection tools (SimHash, MinHash, generic semantic
dedup pipelines) treat all duplicates identically. The register-aware split
is what makes this a Sinhala-corpus-engineering contribution rather than a
generic semantic dedup step.

What this stage is NOT
----------------------
- It is NOT a full document-level topic model. It compares whole documents
  as single strings, which is fine for typical news/OCR/ASR paragraph-length
  content but would miss partial overlap in very long documents.
- It does NOT re-encode previously-seen documents. Once a document is stored,
  its embedding is fixed. Adding new suffix rules to the analyzer doesn't
  affect stored embeddings — they only depend on the underlying model.
- It does NOT handle streaming batch parallelism. The embedding call is
  per-document; for high-throughput deployment, batch-encoding would be a
  separate optimization.

Implementation notes
--------------------
- LaBSE covers 109 languages including Sinhala. It's the accuracy pick.
  For a smaller/faster alternative, pass "sentence-transformers/paraphrase-
  multilingual-MiniLM-L12-v2" to the constructor — no other code changes.
- Embeddings are stored as L2-normalized (unit-length) vectors so cosine
  similarity reduces to a single dot product — a batched numpy matmul.
- Documents that were already REJECTED by earlier stages are skipped: they
  don't get embedded, and their embedding isn't stored. This saves compute
  and keeps the seen-embedding store clean.

Requires:  sentence-transformers  (pip install sentence-transformers)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from ..schema import Document, Verdict
from ..stages.base import Stage

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


# Default model. LaBSE — 109 languages, ~470MB, higher accuracy on cross-lingual
# similarity than smaller MiniLM alternatives. Swap for a smaller model by
# passing `model_name=` to the constructor.
DEFAULT_MODEL = "sentence-transformers/LaBSE"

# Threshold for calling two documents "semantically overlapping". Calibrated
# roughly to: paraphrases of the same content usually score > 0.85; loosely
# related documents on the same topic score 0.55–0.75; unrelated documents
# score < 0.5. Will be re-tuned once we have labeled pairs.
DEFAULT_THRESHOLD = 0.85

# Minimum length in characters below which we don't embed. Very short strings
# produce unstable embeddings that misleadingly match a lot of other short
# strings. Empirically, LaBSE is unreliable below ~30 characters.
MIN_CHARS_TO_EMBED = 30


class CrossRegisterSemanticOverlap(Stage):
    """
    Detect semantic overlap between documents using sentence embeddings,
    with register-aware verdict routing.

    Stateful: builds up a store of (doc_id, source, embedding) across
    documents in a batch. Call `reset()` between independent batches.
    """

    name = "quality.cross_register_overlap"

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        threshold: float = DEFAULT_THRESHOLD,
        min_chars: int = MIN_CHARS_TO_EMBED,
    ) -> None:
        self.model_name = model_name
        self.threshold = threshold
        self.min_chars = min_chars

        # Lazy-loaded so importing the module doesn't trigger a 470MB download.
        # The model is only fetched on first use.
        self._model: SentenceTransformer | None = None

        # Storage for seen documents. Kept as three parallel structures
        # rather than a list of tuples so we can vectorise the similarity
        # search with a single matmul.
        self._seen_ids: list[str] = []
        self._seen_sources: list[str] = []
        self._seen_embeddings: list[np.ndarray] = []

    # ----- Lifecycle ------------------------------------------------------

    def reset(self) -> None:
        """Clear seen documents. Useful between independent batches."""
        self._seen_ids.clear()
        self._seen_sources.clear()
        self._seen_embeddings.clear()

    def _get_model(self) -> SentenceTransformer:
        """Load the embedding model on first use, then cache it."""
        if self._model is None:
            # Deferred import: keeps sentence-transformers optional for anyone
            # who's only using earlier stages, and keeps module import cheap.
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
        return self._model

    # ----- Main processing -----------------------------------------------

    def _process(self, doc: Document) -> Document:
        # Skip if an earlier stage already rejected this document. No point
        # embedding rejected content or letting it pollute the seen store.
        if doc.verdict == Verdict.REJECT:
            doc.quality["semantic_overlap"] = {
                "skipped_reason": "already_rejected_upstream",
            }
            return doc

        # Skip very short documents — embeddings on <30 chars are unstable.
        if len(doc.text) < self.min_chars:
            doc.quality["semantic_overlap"] = {
                "skipped_reason": f"too_short:{len(doc.text)}<{self.min_chars}",
            }
            return doc

        # Compute this document's embedding.
        model = self._get_model()
        embedding = model.encode(
            doc.text,
            normalize_embeddings=True,  # unit length → dot product == cosine sim
            show_progress_bar=False,
        )
        # `encode` on a single string returns a 1-D array of shape (dim,).
        embedding = np.asarray(embedding, dtype=np.float32)

        # Find the best match against previously-seen documents (if any).
        best = self._find_best_match(embedding)

        # Record what we found (regardless of whether it triggers a verdict).
        overlap_info: dict = {
            "embedded": True,
            "best_match_id": best["id"] if best else None,
            "best_match_source": best["source"] if best else None,
            "best_match_score": round(best["score"], 3) if best else None,
            "threshold": self.threshold,
        }

        # Register-aware verdict logic.
        if best and best["score"] >= self.threshold:
            if best["source"] == doc.source.value:
                # Same-source high-similarity → near-duplicate. Reject.
                doc.verdict = Verdict.REJECT
                doc.verdict_reasons.append(
                    f"semantic_duplicate_of:{best['id']}:{best['score']:.2f}"
                )
                overlap_info["decision"] = "reject_same_source"
            else:
                # Cross-register overlap → route to REVIEW unless already REJECT.
                # We don't override an existing REJECT (though we shouldn't
                # have gotten this far anyway — safety guard).
                if doc.verdict != Verdict.REJECT:
                    doc.verdict = Verdict.REVIEW
                doc.verdict_reasons.append(
                    f"cross_register_overlap:{best['id']}:"
                    f"{best['source']}→{doc.source.value}:{best['score']:.2f}"
                )
                overlap_info["decision"] = "review_cross_register"
        else:
            overlap_info["decision"] = "no_overlap"

        doc.quality["semantic_overlap"] = overlap_info

        # Store this document's embedding for future comparisons.
        # We store it AFTER the comparison, so a doc doesn't match itself.
        self._seen_ids.append(doc.doc_id)
        self._seen_sources.append(doc.source.value)
        self._seen_embeddings.append(embedding)

        return doc

    # ----- Similarity search --------------------------------------------

    def _find_best_match(self, embedding: np.ndarray) -> dict | None:
        """
        Return the best-matching prior document (or None if none seen yet).

        Because all embeddings are unit-length, cosine similarity is just
        a dot product. Stacking stored embeddings into a single matrix and
        doing one matmul is much faster than looping — even at prototype
        scales, this matters when the seen-corpus grows.
        """
        if not self._seen_embeddings:
            return None

        # Stack stored embeddings into a matrix of shape (N, dim).
        stored = np.stack(self._seen_embeddings, axis=0)

        # Single matmul: (N, dim) @ (dim,) → (N,) of cosine similarities.
        similarities = stored @ embedding

        # Best match.
        best_idx = int(np.argmax(similarities))
        best_score = float(similarities[best_idx])

        return {
            "id": self._seen_ids[best_idx],
            "source": self._seen_sources[best_idx],
            "score": best_score,
        }
