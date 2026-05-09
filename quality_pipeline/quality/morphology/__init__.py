"""
Morphology-aware Sinhala quality scoring.

This is the novelty contribution of the quality pipeline: a Sinhala-specific
scorer that exploits agglutinative morphology to detect text degradation
(OCR errors, ASR disfluencies, low-quality MT) that language-agnostic
quality scorers miss.
"""


from .analyzer import Morpheme, MorphemeAnalysis, analyze, analyze_text
from .scorer import MorphologyQualityScorer
from .suffix_rules import CATEGORIES, SUFFIX_TABLE

__all__ = [
    "Morpheme",
    "MorphemeAnalysis",
    "MorphologyQualityScorer",
    "analyze",
    "analyze_text",
    "CATEGORIES",
    "SUFFIX_TABLE",
]
