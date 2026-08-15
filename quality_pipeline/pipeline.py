"""
pipeline.py
-----------
The orchestrator. Holds an ordered list of Stages and runs a Document through them.
"""

import logging
from typing import Iterable

from .schema import Document, Verdict
from .stages.base import Stage

log = logging.getLogger(__name__)


class QualityPipeline:
    """Runs a Document through an ordered list of Stages."""

    def __init__(self, stages: list[Stage]):
        if not stages:
            raise ValueError("QualityPipeline needs at least one stage")
        self.stages = stages

    def run(self, doc: Document) -> Document:
        """Process one document through every stage in order."""
        for stage in self.stages:
            try:
                doc = stage.process(doc)
            except Exception as e:
                log.exception("Stage %s failed on doc %s", stage.name, doc.doc_id)
                doc.verdict = Verdict.REJECT
                doc.verdict_reasons.append(f"stage_error:{stage.name}:{type(e).__name__}")
                break
        return doc

    def run_batch(self, docs: Iterable[Document]) -> list[Document]:
        """Convenience: process many docs, return the list of processed ones."""
        return [self.run(doc) for doc in docs]