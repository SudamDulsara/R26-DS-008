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

  - Similarity > threshold AND same source AND polarity matches
        → REJECT (near-duplicate)
  - Similarity > threshold AND different source AND polarity matches
        → REVIEW (cross-register overlap)
  - Similarity > threshold BUT polarity mismatch (one negated, one not)
        → NO ACTION (opposite claims — not duplicates)
  - Similarity ≤ threshold
        → NO ACTION

Why the polarity check exists
-----------------------------
Sentence-embedding models like LaBSE are trained on paraphrase similarity.
They are structurally weak at negation: two sentences that differ only by
"X did Y" vs "X did NOT Y" often score >0.85 similar because 90%+ of the
words are identical, and the training objective wasn't sensitive to that
specific perturbation. This is a documented limitation across sentence-BERT
variants (see SICK-R and STS-B analyses).

For a corpus quality pipeline, this matters: a news article stating
"the president approved the bill" and one stating "the president did NOT
approve the bill" are NOT duplicates — they make opposite claims and both
belong in the training corpus.

The polarity check is a small Sinhala-specific guard implemented in
`negation_detector.py`. It detects standalone negation markers (නැත, නැහැ,
නෑ, නොවේ) and productive නො- prefixed verbs (නොකළ, නොදකින), based on
De Abrew (1981) and Peiris (2018). It's a coarse binary check, not a
full negation-scope analyzer — but it catches the specific failure mode
(direct clausal negation of matched content) that generic sentence
encoders miss.

Why this is the second novelty
------------------------------
Stage 2 (exact-hash dedup) catches only byte-identical content after
normalization. It cannot catch paraphrases: "the prime minister announced X"
vs "today's announcement from the PM was X" produce completely different
hashes. This stage catches them.

Beyond generic semantic dedup, this stage is REGISTER-AWARE and
POLARITY-AWARE — using both the `source` field and Sinhala-specific
negation detection to make finer-grained decisions than a threshold-only
approach. This combination is what makes it a Sinhala-corpus-engineering
contribution rather than a generic semantic dedup step.

Requires:  sentence-transformers  (pip install sentence-transformers)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from ..schema import Document, Verdict
from ..stages.base import Stage
from .negation_detector import polarity_matches

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


# Default model. LaBSE — 109 languages, ~470MB, higher accuracy on cross-lingual
# similarity than smaller MiniLM alternatives.
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
    with register-aware AND polarity-aware verdict routing.

    Stateful: builds up a store of (doc_id, source, text, embedding) across
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

        self._model: SentenceTransformer | None = None

        # Storage for seen documents. Parallel lists so we can vectorise
        # the similarity search with a single matmul.
        # `_seen_texts` is new: we keep it so the polarity check can look
        # at the ACTUAL text of the best-matched prior doc, not just its
        # embedding.
        self._seen_ids: list[str] = []
        self._seen_sources: list[str] = []
        self._seen_texts: list[str] = []
        self._seen_embeddings: list[np.ndarray] = []

    # ----- Lifecycle ------------------------------------------------------

    def reset(self) -> None:
        """Clear seen documents. Useful between independent batches."""
        self._seen_ids.clear()
        self._seen_sources.clear()
        self._seen_texts.clear()
        self._seen_embeddings.clear()

    def _get_model(self) -> SentenceTransformer:
        """Load the embedding model on first use, then cache it."""
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
        return self._model

    # ----- Main processing -----------------------------------------------

    def _process(self, doc: Document) -> Document:
        # Skip if an earlier stage already rejected this document.
        if doc.verdict == Verdict.REJECT:
            doc.quality["semantic_overlap"] = {
                "skipped_reason": "already_rejected_upstream",
            }
            return doc

        # Skip very short documents.
        if len(doc.text) < self.min_chars:
            doc.quality["semantic_overlap"] = {
                "skipped_reason": f"too_short:{len(doc.text)}<{self.min_chars}",
            }
            return doc

        # Compute this document's embedding.
        model = self._get_model()
        embedding = model.encode(
            doc.text,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        embedding = np.asarray(embedding, dtype=np.float32)

        # Find the best match against previously-seen documents.
        best = self._find_best_match(embedding)

        overlap_info: dict = {
            "embedded": True,
            "best_match_id": best["id"] if best else None,
            "best_match_source": best["source"] if best else None,
            "best_match_score": round(best["score"], 3) if best else None,
            "threshold": self.threshold,
        }

        # Register-aware AND polarity-aware verdict logic.
        if best and best["score"] >= self.threshold:
            best_text = self._seen_texts[best["idx"]]

            if not polarity_matches(doc.text, best_text):
                # Sentence-embedding similarity is high but negation polarity
                # differs — this is a well-known failure mode of sentence
                # encoders. Two docs making opposite claims are NOT duplicates.
                overlap_info["decision"] = "no_overlap_polarity_mismatch"
                overlap_info["polarity_note"] = (
                    "similarity above threshold but negation polarity differs "
                    "— treated as distinct content"
                )
            elif best["source"] == doc.source.value:
                # Same-source, similar meaning, matching polarity → duplicate.
                doc.verdict = Verdict.REJECT
                doc.verdict_reasons.append(
                    f"semantic_duplicate_of:{best['id']}:{best['score']:.2f}"
                )
                overlap_info["decision"] = "reject_same_source"
            else:
                # Cross-register overlap → REVIEW.
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

        # Store this document's embedding + text for future comparisons.
        self._seen_ids.append(doc.doc_id)
        self._seen_sources.append(doc.source.value)
        self._seen_texts.append(doc.text)
        self._seen_embeddings.append(embedding)

        return doc

    # ----- Similarity search --------------------------------------------

    def _find_best_match(self, embedding: np.ndarray) -> dict | None:
        """
        Return the best-matching prior document (or None if none seen yet).
        Includes `idx` so the caller can look up the text for polarity check.
        """
        if not self._seen_embeddings:
            return None

        stored = np.stack(self._seen_embeddings, axis=0)
        similarities = stored @ embedding

        best_idx = int(np.argmax(similarities))
        best_score = float(similarities[best_idx])

        return {
            "idx": best_idx,
            "id": self._seen_ids[best_idx],
            "source": self._seen_sources[best_idx],
            "score": best_score,
        }