"""Stage 3 — Quality Pipeline. Deduplication, scoring, safety filtering, routing."""
from .deduplicator import ExactHashDeduplicator
from .semantic_overlap import CrossRegisterSemanticOverlap

__all__ = ["ExactHashDeduplicator", "CrossRegisterSemanticOverlap"]