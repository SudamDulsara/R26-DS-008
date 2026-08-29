"""
Continuous Dataset Generation — Sri Lanka Government Gazette
=============================================================
Harvests the Sinhala Government Gazette and turns each issue into
(raw OCR text -> correct text) training pairs, automatically.

    python build_gazette_pairs.py --weeks 8      # last 8 issues
    python build_gazette_pairs.py --latest       # just this week's

Output : data/_synthetic_degradation/training/gazette_pairs.jsonl
Resume : safe to interrupt; already-processed issues are skipped


WHY THE GAZETTE
---------------
Gazette PDFs are typeset in InDesign, not scanned. The correct Sinhala
text is already inside the file. That gives ground truth for free —
no hand-typing, and no LLM guessing what a damaged word should be.

Published every Friday at a completely predictable URL, so the pipeline
needs no scraping and can run unattended indefinitely. That is what makes
the dataset continuous rather than a one-off collection.


HOW ONE PAGE BECOMES TRAINING PAIRS
-----------------------------------
        gazette PDF page
               |
        +------+------+
        |             |
    text layer    render at 200 DPI
        |             |
     clean()      degrade()          <- simulate scanning
        |             |
        |         Tesseract
        |             |
    CORRECT        RAW OCR
        \             /
         one pair per line

Pairing happens per *line*, using the line boxes the PDF itself provides.
Each rendered line is cropped and OCR'd on its own, so the two sides stay
aligned. Line-sized pairs also suit ByT5, which sees Sinhala at three
bytes per character and cannot take whole pages.


THE TEXT-LAYER CLEANUP
----------------------
Extracted Sinhala arrives with two systematic defects: vowel signs are
duplicated (කොටස -> කොˁොටස) and stray Latin/IPA glyphs appear inside
words. clean() removes both. Measured against the project's 116,770-word
Sinhala dictionary, this lifts valid-word rate from 78.2% to 98.6%.
"""

import argparse
import io
import json
import os
import random
import re
import sys
import time
import unicodedata
from datetime import date, timedelta

import pymupdf
import requests
from PIL import Image, ImageFilter

from pipeline import ocr_engine  # sets the Tesseract path as a side effect
import pytesseract

from pipeline.normalize import normalize

OUT_PATH = os.path.join("data", "_synthetic_degradation", "training", "gazette_pairs.jsonl")
PDF_CACHE = os.path.join("data", "_synthetic_degradation", "gazette_pdfs")

URL_TEMPLATE = "https://www.gazette.lk/dl/Gazette/{mm}/Gazette-{yyyy}-{mm}-{dd}-Sii.pdf"

RENDER_DPI = 200          # typical scanner resolution
OCR_LANG = "sin"
OCR_CONFIG = r"--oem 3 --psm 7"   # psm 7 = treat the crop as a single line

MIN_CHARS = 15            # ignore page numbers and stray fragments
MIN_SINHALA_RATIO = 0.60  # ignore English-only lines and tables of numbers

#: Sinhala marks that the PDF text layer duplicates. Two identical marks
#: never occur in a row in real Sinhala, so collapsing them is safe.
DUPLICATED_MARKS = "ංඃ්-ෟෲෳ"

#: Typographic punctuation that genuinely appears in the documents. These
#: are neither Sinhala nor ASCII, so without this exemption the cleanup
#: deletes them from the correct text while Tesseract still reads them —
#: which would train the model to strip quotation marks and dashes.
KEEP_PUNCTUATION = "“”‘’–—…‚„«»"


# ─────────────────────────────────────────────────────────────
#  Text-layer cleanup
# ─────────────────────────────────────────────────────────────

def clean(text: str) -> str:
    """Repair the two systematic defects in the extracted text layer."""
    text = "".join(
        c for c in text
        if ("඀" <= c <= "෿") or c.isascii() or c in KEEP_PUNCTUATION or c in "‌‍\n\t "
    )
    text = re.sub(rf"([{DUPLICATED_MARKS}])\1+", r"\1", text)
    return normalize(unicodedata.normalize("NFC", text))


def sinhala_ratio(text: str) -> float:
    chars = [c for c in text if not c.isspace()]
    if not chars:
        return 0.0
    return sum(1 for c in chars if "඀" <= c <= "෿") / len(chars)


# ─────────────────────────────────────────────────────────────
#  Make a rendered page look like a scan
# ─────────────────────────────────────────────────────────────

def degrade(img: Image.Image, rng: random.Random) -> Image.Image:
    """
    Rendered pages are unnaturally clean. Real scans carry blur, sensor
    noise and JPEG artifacts, and those change which mistakes the OCR
    makes. Without this the model would only ever learn to fix errors
    from perfect printing.

    Deliberately mild — heavy damage produces OCR output too corrupted to
    be worth correcting, which teaches the model nothing.
    """
    img = img.convert("L")

    if rng.random() < 0.7:
        img = img.filter(ImageFilter.GaussianBlur(rng.uniform(0.3, 0.8)))

    if rng.random() < 0.7:
        px = img.load()
        w, h = img.size
        for _ in range(int(w * h * 0.004)):
            x, y = rng.randrange(w), rng.randrange(h)
            px[x, y] = max(0, min(255, px[x, y] + rng.randint(-70, 70)))

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=rng.randint(55, 85))
    buf.seek(0)
    return Image.open(buf)


# ─────────────────────────────────────────────────────────────
#  Fetching
# ─────────────────────────────────────────────────────────────

def gazette_url(d: date) -> str:
    return URL_TEMPLATE.format(yyyy=d.year, mm=f"{d.month:02d}", dd=f"{d.day:02d}")


def recent_fridays(n: int) -> list:
    """The last n Fridays, newest first. The Gazette publishes weekly."""
    today = date.today()
    friday = today - timedelta(days=(today.weekday() - 4) % 7)
    return [friday - timedelta(weeks=i) for i in range(n)]


def download(d: date, attempts: int = 4) -> str | None:
    """
    Fetch one issue, caching it so re-runs cost nothing.

    Retries with a growing pause. Requesting many issues back-to-back gets
    the host to start refusing connections, and a failed download is
    indistinguishable from an issue that was never published — so without
    retries the harvester silently skips weeks that do exist.
    """
    os.makedirs(PDF_CACHE, exist_ok=True)
    path = os.path.join(PDF_CACHE, f"gazette-{d.isoformat()}-Sii.pdf")
    if os.path.exists(path) and os.path.getsize(path) > 10_000:
        return path

    for attempt in range(1, attempts + 1):
        try:
            r = requests.get(gazette_url(d), timeout=180)
            if r.status_code == 404:
                return None                       # genuinely not published
            if r.status_code == 200 and len(r.content) >= 10_000:
                with open(path, "wb") as f:
                    f.write(r.content)
                return path
            print(f"    attempt {attempt}: HTTP {r.status_code}")
        except Exception as err:                               # noqa: BLE001
            print(f"    attempt {attempt}: {type(err).__name__}")

        if attempt < attempts:
            time.sleep(5 * attempt)                # 5s, 10s, 15s
    return None


# ─────────────────────────────────────────────────────────────
#  One issue -> pairs
# ─────────────────────────────────────────────────────────────

def pairs_from_pdf(pdf_path: str, issue: str, rng: random.Random):
    """Yield one record per usable line of the issue."""
    doc = pymupdf.open(pdf_path)
    zoom = RENDER_DPI / 72.0

    for pno in range(doc.page_count):
        page = doc[pno]
        try:
            pix = page.get_pixmap(matrix=pymupdf.Matrix(zoom, zoom))
            page_img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        except Exception:                                      # noqa: BLE001
            continue

        for block in page.get_text("dict").get("blocks", []):
            for line in block.get("lines", []):
                gold = clean("".join(s.get("text", "") for s in line.get("spans", [])))

                if len(gold) < MIN_CHARS or sinhala_ratio(gold) < MIN_SINHALA_RATIO:
                    continue

                # Crop the rendered page to this line's own box, so the OCR
                # text and the PDF text describe exactly the same words.
                x0, y0, x1, y1 = line["bbox"]
                pad = 4
                box = (
                    max(0, int(x0 * zoom) - pad),
                    max(0, int(y0 * zoom) - pad),
                    min(page_img.width, int(x1 * zoom) + pad),
                    min(page_img.height, int(y1 * zoom) + pad),
                )
                if box[2] - box[0] < 40 or box[3] - box[1] < 12:
                    continue

                crop = degrade(page_img.crop(box), rng)
                try:
                    ocr = normalize(
                        pytesseract.image_to_string(crop, lang=OCR_LANG, config=OCR_CONFIG)
                    )
                except Exception:                              # noqa: BLE001
                    continue

                if not ocr.strip():
                    continue

                yield {
                    "issue": issue,
                    "page": pno + 1,
                    "ocr": ocr,
                    "gold": gold,
                    "sinhala_ratio": round(sinhala_ratio(ocr), 4),
                    "gold_len": len(gold),
                }


# ─────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────

def processed_issues(path: str) -> set:
    """Issues already in the output file, so a re-run skips them."""
    if not os.path.exists(path):
        return set()
    seen = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                seen.add(json.loads(line)["issue"])
            except Exception:                                  # noqa: BLE001
                pass
    return seen


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--weeks", type=int, default=4, help="how many recent issues")
    ap.add_argument("--latest", action="store_true", help="only the most recent issue")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    dates = recent_fridays(1 if args.latest else args.weeks)
    done = processed_issues(OUT_PATH)
    if done:
        print(f"{len(done)} issue(s) already processed — skipping those.\n")

    total = 0
    with open(OUT_PATH, "a", encoding="utf-8", buffering=1) as out:
        for d in dates:
            issue = d.isoformat()
            if issue in done:
                continue

            time.sleep(3)   # be polite to the host between issues
            print(f"[{issue}] downloading ...")
            pdf = download(d)
            if not pdf:
                print("    not published / unavailable — skipping")
                continue

            n = 0
            for rec in pairs_from_pdf(pdf, issue, rng):
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n += 1
            total += n
            print(f"    {n:,} pairs")

    print(f"\nDone. {total:,} new pair(s) -> {OUT_PATH}")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
