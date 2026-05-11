"""
scorer.py
---------
Stage 3b: Morphology-aware quality scoring for Sinhala documents.

What this stage does
--------------------
For each document, it analyzes every word with the morphological analyzer
and computes a small set of features that reflect the *morphological
richness* of the text. These features are then combined into a 0–100
quality score, and a per-feature breakdown is recorded so the frontend
can show *why* a document scored what it did.

Why morphology-based quality scoring is novel for Sinhala
--------------------------------------------------------
1. Existing Sinhala morphological analyzers (Hettige & Karunananda 2006;
   Welgama et al. 2013; SinMorphy 2021) were all built to support
   *machine translation* or *parsing* — they consume morphology, they
   don't measure it. None of them are used to score corpus quality.

2. Generic LLM-corpus quality scorers (CCNet, fastText perplexity filters,
   character-ratio heuristics, length thresholds) are *language-agnostic*.
   They cannot exploit Sinhala's agglutinative structure, so they miss
   degradation patterns specific to it:
     - OCR garbage often produces letter sequences that don't end in any
       known case/verb suffix → low decomposition rate.
     - ASR disfluencies repeat a small morpheme inventory → low diversity.
     - Low-quality machine-translated text often has flat, formulaic
       morphology → low agglutination depth and low category diversity.
   A morphology-aware scorer detects these by construction.

3. We're not just counting morphemes; we're measuring *category diversity*
   (how many different *kinds* of morphological operations the text uses).
   This is the part that's specific to agglutinative languages and is
   absent from prior corpus-quality work for Sinhala.

What the score is NOT
---------------------
It is *not* a substitute for human review. It's one signal among several
(dedup, length, language ID, safety) that together drive the final
accept/review/reject verdict. A document with low morphological richness
might still be valid — a list of place names, a math problem, a song lyric. 
The router (Stage 4) is what makes the actual decision.
"""

from collections import Counter

from ...schema import Document, Verdict
from ...stages.base import Stage
from .analyzer import MorphemeAnalysis, analyze_text
from .suffix_rules import CATEGORIES


# Score thresholds for routing. Calibrated against the 31-doc test corpus:
#   - Garbage / mostly-non-Sinhala docs cluster at 0–10
#   - Disfluent / sparse Sinhala clusters at 30–45
#   - Well-formed Sinhala news/OCR/ASR clusters at 45–65
# Will be re-tuned once we have human-labeled data.
ACCEPT_THRESHOLD = 45.0   # >= this → ACCEPT
REJECT_THRESHOLD = 15.0   # <  this → REJECT (else REVIEW)

# Minimum word count to even attempt scoring. Below this, no signal.
MIN_WORDS_TO_SCORE = 4


class MorphologyQualityScorer(Stage):
    """Score documents by Sinhala morphological richness."""

    name = "quality.morphology_score"

    def _process(self, doc: Document) -> Document:
        analyses = analyze_text(doc.text)
        features = self._compute_features(analyses)
        score = self._combine_score(features) if features["scoreable"] else 0.0

        doc.quality["morphology"] = {
            "features": features,
            "score": round(score, 1),
            "per_word": [self._summarize_word(a) for a in analyses],
        }

        # NEW: docs with no Sinhala at all are rejected regardless of why we couldn't score them. 
        # The pipeline produces a Sinhala corpus; non-Sinhala has no place in it.
        
        if features["sinhala_words"] == 0:
            doc.verdict = Verdict.REJECT
            doc.verdict_reasons.append("no_sinhala_content")
            return doc

        # Docs that are too short to score reliably go to REVIEW, not ACCEPT.
        # A human can decide; we shouldn't tentatively accept on no information.
        if not features["scoreable"]:
            if doc.verdict == Verdict.PENDING:
                doc.verdict = Verdict.REVIEW
                doc.verdict_reasons.append(
                    f"too_short_to_score:{features['sinhala_words']}_sinhala_words"
                )
            return doc

        # Existing scoring-based routing.
        if score < REJECT_THRESHOLD:
            doc.verdict = Verdict.REJECT
            doc.verdict_reasons.append(f"low_morphology_score:{score:.1f}<{REJECT_THRESHOLD}")
        elif score < ACCEPT_THRESHOLD:
            if doc.verdict == Verdict.PENDING:
                doc.verdict = Verdict.REVIEW
                doc.verdict_reasons.append(f"borderline_morphology:{score:.1f}")

        return doc
    # --- Feature computation ----------------------------------------------

    def _compute_features(self, analyses: list[MorphemeAnalysis]) -> dict:
        """
        Produce a dict of features describing the document's morphology.
        Each feature is in [0, 1] for easy weighted combination, except
        counts which are reported alongside.
        """
        sinhala_words = [a for a in analyses if a.is_sinhala]
        total = len(analyses)
        sinhala_count = len(sinhala_words)

        # Trivial / non-scoreable cases.
        if total < MIN_WORDS_TO_SCORE or sinhala_count == 0:
            return {
                "scoreable": False,
                "total_words": total,
                "sinhala_words": sinhala_count,
                "sinhala_ratio": (sinhala_count / total) if total else 0.0,
                "decomposition_rate": 0.0,
                "agglutination_depth": 0.0,
                "morpheme_diversity": 0.0,
                "category_diversity": 0.0,
            }

        # 1. Sinhala ratio — share of tokens that are Sinhala at all.
        sinhala_ratio = sinhala_count / total

        # 2. Decomposition rate — share of Sinhala words that decomposed
        #    into root + at least one recognized suffix.
        #    Sinhala has uninflected words too, so we don't expect 100%.
        #    Empirically, well-formed Sinhala news text sits ~50–75%.
        decomposed = [a for a in sinhala_words if a.decomposed]
        decomposition_rate = len(decomposed) / sinhala_count

        # 3. Agglutination depth — average number of suffixes per Sinhala
        #    word. Higher = more morphologically rich. We normalize to [0,1]
        #    by capping at 3.0 (3+ suffixes is plenty rich).
        avg_depth = sum(a.depth for a in sinhala_words) / sinhala_count
        agglutination_depth = min(avg_depth / 3.0, 1.0)

        # 4. Morpheme diversity — type/token ratio of suffixes used.
        #    Low TTR = repetitive (e.g. ASR disfluencies repeating "හරි").
        #    High TTR = varied morphological operations.
        all_suffixes = [m.surface for a in sinhala_words for m in a.suffixes]
        if all_suffixes:
            morpheme_diversity = len(set(all_suffixes)) / len(all_suffixes)
        else:
            morpheme_diversity = 0.0

        # 5. Category diversity — how many of the 5 categories appear at all.
        #    A doc using ALL of {case, plural, definite, verb, clitic} is
        #    morphologically richer than one using only verbs.
        used_categories = set()
        for a in sinhala_words:
            used_categories |= a.categories
        category_diversity = len(used_categories) / len(CATEGORIES)

        return {
            "scoreable": True,
            "total_words": total,
            "sinhala_words": sinhala_count,
            "sinhala_ratio": round(sinhala_ratio, 3),
            "decomposition_rate": round(decomposition_rate, 3),
            "agglutination_depth": round(agglutination_depth, 3),
            "morpheme_diversity": round(morpheme_diversity, 3),
            "category_diversity": round(category_diversity, 3),
            "categories_used": sorted(used_categories),
            "top_suffixes": Counter(all_suffixes).most_common(5),
        }

    # --- Score combination -------------------------------------------------

    # Feature weights. These are *interpretable* defaults — each feature gets
    # weight proportional to how reliably it separates good from bad docs in
    # our test corpus. Will be re-tuned once we have labeled data.
    _WEIGHTS = {
        "sinhala_ratio":        0.20,
        "decomposition_rate":   0.30,
        "agglutination_depth":  0.20,
        "morpheme_diversity":   0.15,
        "category_diversity":   0.15,
    }

    def _combine_score(self, f: dict) -> float:
        """Weighted average of features, scaled to 0–100."""
        return 100.0 * sum(
            self._WEIGHTS[name] * f[name] for name in self._WEIGHTS
        )

    # --- Per-word summary for the frontend --------------------------------

    @staticmethod
    def _summarize_word(a: MorphemeAnalysis) -> dict:
        return {
            "word": a.word,
            "root": a.root,
            "suffixes": [(m.surface, m.category) for m in a.suffixes],
            "is_sinhala": a.is_sinhala,
            "decomposed": a.decomposed,
        }