"""
Build the primary training corpus: real Tesseract errors paired with true
ground truth.

Runs Tesseract over every image in Ransaka/sinhala_synthetic_ocr-large and
records (ocr_output, gold_text) pairs. The gold side is the dataset's own
transcription, so it is correct by construction — no hand-typing, no LLM
labelling, no guessing.

    python build_ocr_pairs.py

Output : data/_synthetic_degradation/training/ocr_pairs.jsonl  (one JSON record per line)
Runtime: ~35 minutes for all 6,969 images (~0.29s each)

Safe to interrupt and re-run — it resumes from wherever it stopped.

Each record carries the two quality signals needed to filter the corpus
later, so filtering decisions can be made and revised without re-running
Tesseract:

  cer            how wrong Tesseract got it, against the gold text
  sinhala_ratio  fraction of the OCR output that is actually Sinhala

`sinhala_ratio` is the one that matters. On roughly 7% of images Tesseract's
script detection fails completely and it emits Latin punctuation and digits
("ය]1[1(81[:)8 (1/1(26)"). Those pairs contain no recoverable signal, and
training on them teaches the model to invent Sinhala out of noise. Filter
them out before training — see filter_pairs() at the bottom of this file.
"""

import json
import os
import sys
import time

# Importing ocr_engine locates the Tesseract binary as a side effect, so the
# OCR settings here stay identical to the ones the live pipeline uses.
from pipeline import ocr_engine  # noqa: F401
import pytesseract

from datasets import load_dataset
from jiwer import cer

from pipeline.normalize import normalize

DATASET  = "Ransaka/sinhala_synthetic_ocr-large"
OUT_PATH = os.path.join("data", "_synthetic_degradation", "training", "ocr_pairs.jsonl")

# Same configuration as pipeline/ocr_engine.py, so the error distribution
# measured here matches what the live pipeline produces.
OCR_LANG   = "sin"
OCR_CONFIG = r"--oem 3 --psm 3"

SINHALA_START, SINHALA_END = 0x0D80, 0x0DFF


def sinhala_ratio(text: str) -> float:
    """Fraction of non-whitespace characters that fall in the Sinhala block."""
    chars = [c for c in text if not c.isspace()]
    if not chars:
        return 0.0
    hits = sum(1 for c in chars if SINHALA_START <= ord(c) <= SINHALA_END)
    return hits / len(chars)


def count_existing(path: str) -> int:
    """Number of records already written, for resuming after an interruption."""
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def main() -> None:
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    done = count_existing(OUT_PATH)
    if done:
        print(f"Resuming — {done:,} record(s) already written.")

    print(f"Loading {DATASET} ...")
    print("(first run downloads ~456 MB to HF_HOME; later runs use the cache)")
    ds = load_dataset(DATASET, split="train")
    total = len(ds)
    print(f"{total:,} images.\n")

    if done >= total:
        print("Already complete.")
        return

    t0 = time.time()
    written = 0

    # Line-buffered append so an interrupted run loses at most one record.
    with open(OUT_PATH, "a", encoding="utf-8", buffering=1) as out:
        for i in range(done, total):
            ex = ds[i]
            gold = normalize(ex["text"])

            img = ex["image"]
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")

            try:
                ocr = normalize(
                    pytesseract.image_to_string(
                        img, lang=OCR_LANG, config=OCR_CONFIG
                    )
                )
            except Exception as err:                      # noqa: BLE001
                print(f"  [{i}] OCR failed: {err}")
                continue

            record = {
                "id":            i,
                "gold":          gold,
                "ocr":           ocr,
                "cer":           round(cer(gold, ocr), 4) if gold.strip() else None,
                "sinhala_ratio": round(sinhala_ratio(ocr), 4),
                "gold_len":      len(gold),
                "ocr_len":       len(ocr),
            }
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1

            if written % 250 == 0:
                elapsed = time.time() - t0
                rate = written / elapsed
                remaining = (total - i - 1) / rate / 60
                print(
                    f"  {i + 1:,}/{total:,}  "
                    f"({rate:.1f} img/s, ~{remaining:.0f} min left)"
                )

    print(f"\nDone. {written:,} record(s) written in "
          f"{(time.time() - t0) / 60:.1f} min → {OUT_PATH}")
    print("\nNext: run filter_pairs() to see how many survive quality filtering.")


def filter_pairs(
    path: str = OUT_PATH,
    min_sinhala_ratio: float = 0.80,
    max_cer: float = 0.90,
) -> dict:
    """
    Report how many pairs survive quality filtering, without writing anything.

    Two rejection criteria, both aimed at pairs with no recoverable signal:

      min_sinhala_ratio  Tesseract's script detection failed and it emitted
                         Latin/digit garbage. The original characters are
                         gone, so no model could recover them.
      max_cer            The OCR output diverges so far from the gold text
                         that the pair teaches invention rather than
                         correction.

    Both defaults are starting points — inspect the rejected samples and
    tune them before committing to a training run.
    """
    kept, dropped_script, dropped_cer, empty = 0, 0, 0, 0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if not r["gold"].strip() or not r["ocr"].strip():
                empty += 1
            elif r["sinhala_ratio"] < min_sinhala_ratio:
                dropped_script += 1
            elif r["cer"] is not None and r["cer"] > max_cer:
                dropped_cer += 1
            else:
                kept += 1

    total = kept + dropped_script + dropped_cer + empty
    return {
        "total":                total,
        "kept":                 kept,
        "dropped_script_fail":  dropped_script,
        "dropped_high_cer":     dropped_cer,
        "dropped_empty":        empty,
        "kept_pct":             round(100 * kept / total, 1) if total else 0.0,
    }


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
