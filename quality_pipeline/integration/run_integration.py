"""
integration/run_integration.py
------------------------------
Incremental quality pipeline runner.

Only NEW documents (not seen in any prior run) are processed. Outputs are
cumulative:

  - verdicts.db                       — cumulative 3-table SQLite
  - training_corpus.jsonl             — cumulative accepted-only corpus
  - pipeline_run_<timestamp>.xlsx     — per-run snapshot
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

from ..io.sqlite_loader import MultiSqliteLoader
from ..io.sqlite_writer import SqliteVerdictWriter
from ..io.training_writer import TrainingJsonlWriter
from ..io.writer import XlsxWriter
from ..linguistic.normalizer import UnicodeNormalizer
from ..pipeline import QualityPipeline
from ..quality.deduplicator import ExactHashDeduplicator
from ..quality.morphology.scorer import MorphologyQualityScorer
from ..quality.semantic_overlap import CrossRegisterSemanticOverlap
from ..schema import Source, Verdict
from .incremental_state import IncrementalState, new_run_id


DEFAULT_INBOX = Path("data/inbox")
DEFAULT_OUTBOX = Path("data/outbox")


def build_pipeline() -> QualityPipeline:
    return QualityPipeline(stages=[
        UnicodeNormalizer(),
        ExactHashDeduplicator(),
        MorphologyQualityScorer(),
        CrossRegisterSemanticOverlap(),
    ])


def run_incremental(
    inbox: Path,
    outbox: Path,
    pipeline: QualityPipeline,
    force_reprocess: bool = False,
) -> dict:
    log = logging.getLogger(__name__)
    outbox.mkdir(parents=True, exist_ok=True)

    verdicts_db = outbox / "verdicts.db"
    training_jsonl = outbox / "training_corpus.jsonl"
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    excel_report = outbox / f"pipeline_run_{ts}.xlsx"

    run_id = new_run_id()
    started_at = datetime.utcnow().isoformat() + "Z"

    state = IncrementalState(verdicts_db)
    if force_reprocess:
        log.info("Force reprocess mode — ignoring processed_docs.")
        skip_seen: dict[Source, set[str]] = {}
    else:
        skip_seen = {src: state.already_seen(src.value) for src in Source}
        for src, seen in skip_seen.items():
            if seen:
                log.info("Already processed for %s: %d docs (will skip)", src.value, len(seen))

    loader = MultiSqliteLoader(inbox_dir=inbox)

    available = loader.count_available()
    log.info("Upstream availability:")
    for source, count in available.items():
        marker = "MISSING" if count < 0 else f"{count} rows"
        log.info("  %-6s: %s", source.value, marker)

    for stage in pipeline.stages:
        if hasattr(stage, "reset"):
            stage.reset()

    log.info("Running quality pipeline on new documents...")
    docs = pipeline.run_batch(loader.load(skip_seen=skip_seen))

    if not docs:
        log.info("No new documents to process. Run complete.")
        return {
            "run_id": run_id,
            "docs_processed": 0,
            "verdicts": {},
            "training_added": 0,
        }

    for doc in docs:
        if doc.verdict == Verdict.PENDING:
            doc.verdict = Verdict.ACCEPT
            doc.verdict_reasons.append("tentative_accept:no_rejection_signals")

    log.info("Appending %d verdicts to %s", len(docs), verdicts_db)
    verdict_counts = SqliteVerdictWriter(verdicts_db).write(
        docs, run_id=run_id, started_at=started_at,
    )

    log.info("Appending accepted docs to %s", training_jsonl)
    training_added = TrainingJsonlWriter(training_jsonl).write(docs, run_id=run_id)

    log.info("Writing per-run Excel report to %s", excel_report)
    XlsxWriter(excel_report).write(docs)

    to_mark: list[tuple[str, str]] = []
    for doc in docs:
        prefix = f"{doc.source.value}_"
        if doc.doc_id.startswith(prefix):
            upstream_id = doc.doc_id[len(prefix):]
            to_mark.append((doc.source.value, upstream_id))
    state.mark_processed(to_mark, run_id=run_id)

    log.info("Verdict breakdown for this run: %s", verdict_counts)
    log.info("Training corpus grew by %d accepted docs.", training_added)
    log.info("Run %s complete.", run_id)

    return {
        "run_id": run_id,
        "docs_processed": len(docs),
        "verdicts": verdict_counts,
        "training_added": training_added,
        "excel_report": str(excel_report),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inbox", type=Path, default=DEFAULT_INBOX)
    parser.add_argument("--outbox", type=Path, default=DEFAULT_OUTBOX)
    parser.add_argument("--force-reprocess", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    log = logging.getLogger(__name__)
    log.info("Building pipeline...")
    pipeline = build_pipeline()

    summary = run_incremental(
        inbox=args.inbox,
        outbox=args.outbox,
        pipeline=pipeline,
        force_reprocess=args.force_reprocess,
    )
    log.info("Final summary: %s", summary)


if __name__ == "__main__":
    main()
