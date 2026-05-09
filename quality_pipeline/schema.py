"""
schema.py
---------
Defines the Document object: the single data contract that flows through
every stage of the quality pipeline.

Every stage accepts a Document, mutates/annotates it, and passes it on.
Need a new field, add it here once — not in each stage.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class Source(str, Enum):
    """Where did this document come from? Needed for source-specific logic later."""
    NEWS = "news"      # from Dulsara's news_pipeline
    OCR = "ocr"        # from Perera's OCR pipeline
    ASR = "asr"        # from the ASR pipeline
    UNKNOWN = "unknown"


class Verdict(str, Enum):
    """Final routing decision produced by Stage 3."""
    ACCEPT = "accept"   # goes into the training corpus
    REVIEW = "review"   # borderline, needs manual check
    REJECT = "reject"   # logged for analytics, dropped
    PENDING = "pending" # pipeline hasn't decided yet


@dataclass
class Document:
    """
    The envelope that carries a single piece of text through the pipeline.

    Stages only *add* information to it — they don't replace the original text
    unless they're explicitly supposed to (e.g., Unicode normalization).
    Keeping raw_text separate from text lets us audit what changed.
    """

    # --- Identity ---
    doc_id: str                        # unique ID, usually from the upstream source
    source: Source                     # news / ocr / asr / unknown

    # --- The actual content ---
    raw_text: str                      # original text as received, never modified
    text: str                          # working copy, stages mutate this

    # --- Upstream metadata (whatever the source gave us) ---
    source_metadata: dict[str, Any] = field(default_factory=dict)
    # e.g., {"url": "...", "published_at": "...", "category": "sports"} for news

    # --- Stage annotations (filled in as the document travels) ---
    linguistic: dict[str, Any] = field(default_factory=dict)
    # filled by Stage 1: {"unicode_normalized": True, "language": "si", ...}

    domain: dict[str, Any] = field(default_factory=dict)
    # filled by Stage 2: {"category": "news.sports", "formality": "formal", ...}

    quality: dict[str, Any] = field(default_factory=dict)
    # filled by Stage 3: {"score": 0.87, "dedup_hash": "...", "flags": [...]}

    # --- Final verdict ---
    verdict: Verdict = Verdict.PENDING
    verdict_reasons: list[str] = field(default_factory=list)

    # --- Audit trail ---
    stages_completed: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def mark_stage_done(self, stage_name: str) -> None:
        """Called by each stage when it finishes — useful for debugging pipelines."""
        self.stages_completed.append(stage_name)

    def __repr__(self) -> str:
        preview = self.text[:40].replace("\n", " ")
        return (
            f"Document(id={self.doc_id!r}, source={self.source.value}, "
            f"verdict={self.verdict.value}, text={preview!r}...)"
        )