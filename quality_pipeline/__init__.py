"""
Sinhala NLP Engine and Unified Quality Pipeline (Component 4).

Receives raw text from upstream components (news crawler, OCR, ASR) and
produces clean, annotated, training-ready Sinhala text.
"""

from .pipeline import QualityPipeline
from .schema import Document, Source, Verdict

__all__ = ["QualityPipeline", "Document", "Source", "Verdict"]