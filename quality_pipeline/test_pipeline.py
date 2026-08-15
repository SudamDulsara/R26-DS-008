"""
test_pipeline.py
----------------
End-to-end smoke test for the quality pipeline.

Run from the repo root:
    python -m quality_pipeline.test_pipeline

What it does:
1. Loads sample_data/raw_docs.jsonl into Document objects
2. Runs them through all four active stages:
     Stage 1  — Unicode normalization
     Stage 2  — Exact-hash deduplication
     Stage 3a — Morphology-aware quality scoring (novelty 1)
     Stage 3b — Cross-register semantic overlap detection (novelty 2)
3. Promotes any still-PENDING documents to ACCEPT (placeholder Stage 4 router).
4. Prints a per-stage summary to the terminal.
5. Writes a detailed Excel report to results/pipeline_run.xlsx.

Adding a new stage is one line here — the orchestrator, schema, and loader
don't change. That's the payoff of the Stage abstraction.
"""

from collections import Counter
from pathlib import Path

from .io import JsonlLoader, XlsxWriter
from .linguistic import UnicodeNormalizer
from .pipeline import QualityPipeline
from .quality import CrossRegisterSemanticOverlap, ExactHashDeduplicator
from .quality.morphology import MorphologyQualityScorer
from .schema import Verdict


def main() -> None:
    here = Path(__file__).parent
    sample_file = here / "sample_data" / "raw_docs.jsonl"
    output_dir = here.parent / "results"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "pipeline_run.xlsx"

    # Build the pipeline. Adding new stages later is one line each.
    pipeline = QualityPipeline(stages=[
        UnicodeNormalizer(),
        ExactHashDeduplicator(),
        MorphologyQualityScorer(),
        CrossRegisterSemanticOverlap(),
    ])

    print("Loading corpus...")
    loader = JsonlLoader(sample_file)
    print("Running pipeline (first run downloads LaBSE ~470MB — please wait)...")
    docs = pipeline.run_batch(loader.load())

    # Tentative Stage 4 (router placeholder): anything still PENDING → ACCEPT.
    for doc in docs:
        if doc.verdict == Verdict.PENDING:
            doc.verdict = Verdict.ACCEPT
            doc.verdict_reasons.append("tentative_accept:no_rejection_signals")

    # ---- Reporting ----
    verdict_counts = Counter(d.verdict.value for d in docs)
    exact_dups = sum(1 for d in docs if d.quality.get("is_duplicate"))
    chars_removed = sum(d.linguistic.get("chars_removed", 0) for d in docs)

    # Semantic-overlap counters — separate REJECT and REVIEW paths so the
    # register-aware split shows up in the terminal summary.
    sem_reject = sum(
        1 for d in docs
        if d.quality.get("semantic_overlap", {}).get("decision") == "reject_same_source"
    )
    sem_review = sum(
        1 for d in docs
        if d.quality.get("semantic_overlap", {}).get("decision") == "review_cross_register"
    )
    sem_embedded = sum(
        1 for d in docs
        if d.quality.get("semantic_overlap", {}).get("embedded")
    )

    print(f"\nProcessed {len(docs)} documents.")
    print(f"  Stage 1 (Unicode)     — characters removed:      {chars_removed}")
    print(f"  Stage 2 (Exact dedup) — exact duplicates:        {exact_dups}")
    print(f"  Stage 3b (Semantic)   — documents embedded:      {sem_embedded}")
    print(f"                          same-source rejects:     {sem_reject}")
    print(f"                          cross-register reviews:  {sem_review}")
    print(f"  Final verdict breakdown:")
    for verdict, count in sorted(verdict_counts.items()):
        print(f"    {verdict:8s}: {count}")

    XlsxWriter(output_file).write(docs)
    print(f"\nReport written to: {output_file}")


if __name__ == "__main__":
    main()