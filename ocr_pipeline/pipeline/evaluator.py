"""
Evaluator Module
=================
Computes standard OCR quality metrics:
  - CER  (Character Error Rate)
  - WER  (Word Error Rate)

Both metrics use edit distance (Levenshtein) under the hood.
Lower is better.  0.0 = perfect match.

Formula:
  CER = (substitutions + deletions + insertions) / len(reference_chars)
  WER = (substitutions + deletions + insertions) / len(reference_words)
"""

from jiwer import cer, wer


def calculate_cer(hypothesis: str, reference: str) -> float:
    """
    Character Error Rate between hypothesis (OCR output) and reference (gold).
    Returns a float in [0, ∞).  Typically expressed as a percentage.
    """
    if not reference.strip():
        return 0.0
    return round(cer(reference, hypothesis), 4)


def calculate_wer(hypothesis: str, reference: str) -> float:
    """
    Word Error Rate between hypothesis and reference.
    """
    if not reference.strip():
        return 0.0
    return round(wer(reference, hypothesis), 4)


def evaluate(raw_ocr: str, refined: str, gold: str) -> dict:
    """
    Compare raw OCR and refined text against the gold standard.

    Returns a dict with before/after CER and WER so the UI can show
    the improvement achieved by the pipeline.
    """
    return {
        "cer_before": calculate_cer(raw_ocr, gold),
        "cer_after":  calculate_cer(refined,  gold),
        "wer_before": calculate_wer(raw_ocr, gold),
        "wer_after":  calculate_wer(refined,  gold),
        "cer_improvement": round(
            calculate_cer(raw_ocr, gold) - calculate_cer(refined, gold), 4
        ),
        "wer_improvement": round(
            calculate_wer(raw_ocr, gold) - calculate_wer(refined, gold), 4
        ),
    }