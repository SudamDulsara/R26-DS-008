"""
Build the evaluation set from AksharaOCR
=========================================
Turns AksharaOCR's (scanned image -> correct text) annotations into the
(raw OCR text -> correct text) pairs this project is evaluated on.

    python build_test_set.py

Output : data/eval/test_set.jsonl
Runtime: a few minutes


WHY THIS IS THE TEST SET AND THE GAZETTE IS NOT
-----------------------------------------------
Two properties are needed to measure this pipeline honestly, and only
AksharaOCR has both:

  real scanned pages   Gazette pages are rendered from a digital PDF, so
                       they lack the paper texture, shadow and uneven ink
                       that cause OCR to fail on actual documents. Scoring
                       well on rendered pages would not show the model
                       works on scans.

  human transcription  The correct text was typed by people looking at the
                       page, not produced by any model. A test set whose
                       answers came from a model would be measuring the
                       model against itself.

It is also a different source from the training data, which matters more
than it sounds: a model tested on the same source it trained on scores
well by recognising that source's habits rather than by correcting
Sinhala.

AksharaOCR is school-textbook material, so this set measures performance
on clean modern printing. Broader document types (newspapers, forms,
older print) need separate photographs — deliberately kept out of here so
the two can be reported separately.


NOTE ON WHAT IS PRESENT
-----------------------
annotation.csv lists ~22,916 items but the released `selected/` folder
holds 1,080 images, so about 1,078 pairs are actually buildable. That is
ample for evaluation; it would be far too few for training.
"""

import csv
import json
import os
import statistics
import sys

from PIL import Image
from jiwer import cer

from pipeline import ocr_engine  # sets the Tesseract path as a side effect
import pytesseract

from pipeline.normalize import normalize

AKSHARA_ROOT = r"D:\Work\data sets\AksharaOCR_v1.0\Sinhala"
ANNOTATIONS = os.path.join(AKSHARA_ROOT, "annotation.csv")
IMAGE_DIR = os.path.join(AKSHARA_ROOT, "selected")

OUT_PATH = os.path.join("data", "eval", "test_set.jsonl")

# Same settings the live pipeline uses, so the errors measured here are the
# errors the deployed system actually produces. psm 7 = one line per image.
OCR_LANG = "sin"
OCR_CONFIG = r"--oem 3 --psm 7"


def main() -> None:
    if not os.path.exists(ANNOTATIONS):
        sys.exit(f"annotation.csv not found at {ANNOTATIONS}")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    with open(ANNOTATIONS, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    have = set(os.listdir(IMAGE_DIR))
    usable = [r for r in rows if r["image_name"] in have]

    print(f"annotations listed : {len(rows):,}")
    print(f"images present     : {len(have):,}")
    print(f"building           : {len(usable):,} pairs\n")

    written, scores = 0, []
    with open(OUT_PATH, "w", encoding="utf-8", buffering=1) as out:
        for i, r in enumerate(usable, 1):
            gold = normalize(r["Text"])
            if not gold.strip():
                continue

            try:
                img = Image.open(os.path.join(IMAGE_DIR, r["image_name"]))
                if img.mode not in ("RGB", "L"):
                    img = img.convert("RGB")
                ocr = normalize(
                    pytesseract.image_to_string(img, lang=OCR_LANG, config=OCR_CONFIG)
                )
            except Exception as err:                           # noqa: BLE001
                print(f"  {r['image_name']}: {err}")
                continue

            score = cer(gold, ocr)
            scores.append(score)
            out.write(json.dumps({
                "image": r["image_name"],
                "ocr": ocr,
                "gold": gold,
                "cer_tesseract": round(score, 4),
            }, ensure_ascii=False) + "\n")
            written += 1

            if i % 200 == 0:
                print(f"  {i:,}/{len(usable):,}")

    print(f"\nWrote {written:,} pairs -> {OUT_PATH}\n")
    if scores:
        print("Tesseract baseline on this set — the number your model must beat:")
        print(f"  mean CER   : {statistics.mean(scores):.4f}")
        print(f"  median CER : {statistics.median(scores):.4f}")
        print(f"  perfect    : {sum(1 for s in scores if s == 0):,}")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
