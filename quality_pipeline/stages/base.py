"""
stages/base.py
--------------
Abstract base class for every pipeline stage.

Why have a base class at all:
- Forces every stage to have the same interface: stage.process(doc) -> Document
- Lets the orchestrator treat all stages identically (loop + call)
- Gives you a single place to add cross-cutting behavior later
  (timing, error handling, logging, metrics) without touching each stage.
- Having a base class means the orchestrator can treat all stages identically (loop and call),
and when you want to add timing, error handling, or metrics later, you change one method (process) instead of editing 20 stages

Subclassing rule: inherit, set `name`, implement `_process`. Don't override `process`.
"""

from abc import ABC, abstractmethod

from ..schema import Document


class Stage(ABC):
    """Base class for any processing stage."""

    name: str = "unnamed_stage"  # subclasses MUST override this

    def process(self, doc: Document) -> Document:
        """
        Public entry point. The orchestrator calls this.
        Don't override it — override _process instead.
        """
        doc = self._process(doc)
        doc.mark_stage_done(self.name)
        return doc

    @abstractmethod
    def _process(self, doc: Document) -> Document:
        """Actual work goes here. Subclasses implement this."""
        ...