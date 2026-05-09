"""
test_pipeline.py
----------------
End-to-end smoke test for the quality pipeline.

Run from the repo root:
    python -m quality_pipeline.test_pipeline

What it does:
1. Loads sample_data/raw_docs.jsonl into Document objects
2. Runs them through Stage 1 (Unicode normalization) + Stage 3a (exact-hash dedup)
3. Promotes any still-PENDING documents to ACCEPT (placeholder until the full
   Stage 3 quality scorer is built — non-rejected docs tentatively pass)
4. Prints a summary to the terminal
5. Writes a detailed Excel report to results/pipeline_run.xlsx

Adding the dedup stage required exactly ONE line of code change in this file
(adding it to the stages list). That's the payoff of the Stage abstraction —
the orchestrator, schema, and loader didn't change at all.
"""

from collections import Counter
from pathlib import Path

from .pipeline import QualityPipeline
from .schema import Verdict

from .linguistic.normalizer import UnicodeNormalizer
from .quality.deduplicator import ExactHashDeduplicator
from .io.loader import JsonlLoader
from .io.writer import XlsxWriter
from .quality.morphology import MorphologyQualityScorer



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
    ])

    loader = JsonlLoader(sample_file)
    docs = pipeline.run_batch(loader.load())

    # Tentative final routing: anything not explicitly rejected is accepted.
    # Placeholder until the full Stage 3 scorer produces graded verdicts.
    for doc in docs:
        if doc.verdict == Verdict.PENDING:
            doc.verdict = Verdict.ACCEPT
            doc.verdict_reasons.append("tentative_accept:no_rejection_signals")

    verdict_counts = Counter(d.verdict.value for d in docs)
    duplicates = sum(1 for d in docs if d.quality.get("is_duplicate"))
    chars_removed = sum(d.linguistic.get("chars_removed", 0) for d in docs)

    print(f"\nProcessed {len(docs)} documents.")
    print(f"  Characters removed by normalization: {chars_removed}")
    print(f"  Exact duplicates detected:           {duplicates}")
    print(f"  Verdict breakdown:")
    for verdict, count in sorted(verdict_counts.items()):
        print(f"    {verdict:8s}: {count}")

    XlsxWriter(output_file).write(docs)
    print(f"\nReport written to: {output_file}")


if __name__ == "__main__":
    main()