"""
Pipeline B — the deliverable
============================

Watches a folder of scanned PDFs and turns every page into one
(raw OCR, corrected) pair. Runs unattended. Resumes where it stopped. Never
processes the same page twice.

    python -m pipeline_b.generate --input data/inbox
    python -m pipeline_b.generate --input data/inbox --corrector lighton \\
                                  --adapter lightonocr-sinhala-acts
    python -m pipeline_b.generate --status


WHAT "CONTINUOUS" MEANS HERE
----------------------------
Not "downloads from a website". It means the generation loop never stops and
never repeats itself, which is demonstrated by three behaviours:

    1. point it at a folder, walk away, come back to finished pairs
    2. kill it mid-run and start it again -- it carries on rather than
       starting over
    3. add more PDFs -- it processes only the new ones

How PDFs arrive in the folder is a separate and much smaller problem. Today a
person downloads them; later a fetcher can drop them in. The pipeline neither
knows nor cares, which is deliberate: tying the contribution to a scraper for
a JavaScript government site would have put the riskiest unverified thing in
the project on the critical path.


WHAT THE OUTPUT CLAIMS
----------------------
On a scanned Act there is no gold text. The corrected column is model output.
It ships labelled as machine-generated on every single row, and it is never
described as ground truth. The accuracy claim comes from a separate
measurement on 202 human-transcribed pages that this pipeline never touches.
"""

import argparse
import os
import sys
import time
import uuid
from collections import Counter

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from pipeline_b.readers import TesseractReader, build_corrector   # noqa: E402
from pipeline_b.store import Store, file_digest                   # noqa: E402

DEFAULT_INBOX = os.path.join(HERE, "data", "inbox")

#: Rendering resolution for PDF pages.
#:
#: 200 DPI is the usual floor for reliable OCR and it is what the raw side
#: needs -- Tesseract degrades badly below it. The corrector downsamples again
#: internally to whatever its vision encoder was fine-tuned at, so rendering
#: higher here would only cost time.
RENDER_DPI = 200

#: A corrected page this much shorter or longer than the raw page is suspect.
#: The two readers disagree about a page all the time, but not by half.
#: Usually means the model stopped early, looped, or hallucinated a block.
LENGTH_RATIO_LOW = 0.60
LENGTH_RATIO_HIGH = 1.60


def quality_flags(raw: str, corrected: str, corrector_name: str,
                  hit_cap: bool) -> list:
    """
    Cheap checks that a row is worth publishing.

    On production input there is no gold text, so none of this measures
    accuracy -- it cannot. These are proxies whose job is to make bad rows
    findable later rather than silently mixed into the dataset.

    The project's own rule: when correction makes something worse, flag the
    row and report the rate per run. Without gold we cannot know "worse", but
    we can know "suspicious", and an unflagged bad row is the thing to avoid.
    """
    flags = []

    if corrector_name == "passthrough":
        flags.append("no_corrector")

    if not raw.strip():
        flags.append("empty_raw")
    if not corrected.strip():
        flags.append("empty_corrected")

    if hit_cap:
        # The model ran out of room mid-page. The corrected side is a
        # fragment. Publishing it as a complete page would be a lie.
        flags.append("hit_token_cap")

    if raw.strip() and corrected.strip():
        ratio = len(corrected) / len(raw)
        if ratio < LENGTH_RATIO_LOW:
            flags.append("much_shorter")
        elif ratio > LENGTH_RATIO_HIGH:
            flags.append("much_longer")

        if corrected.strip() == raw.strip():
            # Two different readers producing byte-identical output means one
            # of them did not really run.
            flags.append("identical")

    # Repetition loop: a classic failure where a generative model gets stuck
    # emitting the same line. Cheap to detect, invisible in a CER average.
    lines = [ln.strip() for ln in corrected.split("\n") if len(ln.strip()) > 20]
    if lines:
        most_common, count = Counter(lines).most_common(1)[0]
        if count >= 4:
            flags.append("repetition")

    return flags


#: A page with fewer extractable characters than this, and at least one
#: image, is a picture of paper. Scanned pages give ~0; born-digital Act pages
#: measured 526-1295 on real documents.gov.lk downloads.
SCAN_CHARS_PER_PAGE = 50
DIGITAL_CHARS_PER_PAGE = 300


def classify_pdf(doc) -> tuple:
    """
    Is this a scan, or a born-digital document? Returns (kind, chars, images).

    This guard is load-bearing, not a nicety. documents.gov.lk serves both:
    Acts from the 1980s are scans of paper, but recent Acts are typeset PDFs
    with the text already inside them.

    Running Pipeline B over a born-digital Act renders clean typeset text to a
    picture and OCRs the picture. The errors that come out are artefacts of
    rendering, not of scanning real paper -- which is synthetic degradation,
    the exact flaw that got the gazette corpus retired from this project. It
    would quietly fill the dataset with unrealistically easy pairs and there
    would be nothing in the output to show it had happened.

    So it is checked here rather than left to whoever fills the folder.
    """
    chars = images = 0
    for page in doc:
        chars += len(page.get_text().strip())
        images += len(page.get_images(full=True))
    n = doc.page_count or 1
    cpp, ipp = chars / n, images / n

    if cpp < SCAN_CHARS_PER_PAGE and ipp >= 0.9:
        return "scanned", cpp, ipp
    if cpp > DIGITAL_CHARS_PER_PAGE:
        return "digital", cpp, ipp
    return "unclear", cpp, ipp


def iter_pdfs(input_dir: str):
    """Every PDF under the input folder, in a stable order."""
    found = []
    for root, _dirs, files in os.walk(input_dir):
        for name in sorted(files):
            if name.lower().endswith(".pdf"):
                found.append(os.path.join(root, name))
    return sorted(found)


def run(args) -> int:
    import fitz          # pymupdf; imported late so --status needs no deps

    store = Store()
    pdfs = iter_pdfs(args.input)
    if not pdfs:
        print(f"No PDFs found in {args.input}")
        print("Put some scanned Act PDFs there and run again.")
        return 1

    print(f"input      : {args.input}")
    print(f"documents  : {len(pdfs)}")
    print(f"corrector  : {args.corrector}"
          + (f" (adapter: {args.adapter})" if args.adapter else ""))
    print(f"output     : {store.jsonl_path}")
    print(f"             {store.db_path}")

    tesseract = TesseractReader(lang=args.lang)
    print(f"tesseract  : {tesseract.version}")

    corrector = build_corrector(
        args.corrector,
        adapter_dir=args.adapter,
        max_image_edge=args.image_edge,
    ) if args.corrector != "none" else build_corrector("none")

    run_id = uuid.uuid4().hex[:12]
    store.start_run(run_id, args.input, corrector.name, note=args.note)
    print(f"run id     : {run_id}\n")

    written = skipped = flagged = 0
    rejected = rejected_pages = 0
    interrupted = False
    started = time.time()

    try:
        for pdf_path in pdfs:
            doc_id = file_digest(pdf_path)
            name = os.path.relpath(pdf_path, args.input)

            with fitz.open(pdf_path) as doc:
                page_count = doc.page_count
                kind, cpp, ipp = classify_pdf(doc)
                accepted = kind == "scanned" or args.allow_digital

                store.register_document(
                    doc_id, name, page_count, os.path.getsize(pdf_path),
                    kind=kind, chars_per_page=round(cpp, 1),
                    images_per_page=round(ipp, 2), accepted=accepted,
                )

                if not accepted:
                    rejected += 1
                    rejected_pages += page_count
                    print(f"{name}: REJECTED -- {kind} "
                          f"({cpp:.0f} chars/page, {ipp:.1f} images/page), "
                          f"{page_count} pages not processed")
                    continue

                if kind == "unclear":
                    print(f"{name}: WARNING -- could not classify "
                          f"({cpp:.0f} chars/page, {ipp:.1f} images/page); "
                          f"processing anyway, check the output")

                done = store.already_done(doc_id)

                todo = [p for p in range(page_count) if p + 1 not in done]
                if not todo:
                    print(f"{name}: all {page_count} pages already done, skipping")
                    skipped += page_count
                    continue

                print(f"{name}: {page_count} pages, {len(done)} done, "
                      f"{len(todo)} to do")

                for page_index in todo:
                    if args.limit and written >= args.limit:
                        raise KeyboardInterrupt("page limit reached")

                    page_num = page_index + 1
                    pix = doc[page_index].get_pixmap(dpi=RENDER_DPI)
                    from PIL import Image
                    image = Image.frombytes(
                        "RGB", (pix.width, pix.height), pix.samples
                    )

                    raw, ocr_s = tesseract.read(image)
                    corrected, corr_s = corrector.read(image, raw_text=raw)
                    hit_cap = getattr(corrector, "hit_cap", False)

                    flags = quality_flags(raw, corrected, corrector.name, hit_cap)
                    ratio = (len(corrected) / len(raw)) if raw else None

                    ok = store.save_pair({
                        "doc_id": doc_id, "source_file": name,
                        "page_num": page_num, "run_id": run_id,
                        "raw_text": raw, "corrected_text": corrected,
                        "corrector": corrector.name,
                        "raw_chars": len(raw), "corrected_chars": len(corrected),
                        "length_ratio": ratio, "flags": flags,
                        "ocr_seconds": round(ocr_s, 2),
                        "correct_seconds": round(corr_s, 2),
                    })

                    if ok:
                        written += 1
                        if flags:
                            flagged += 1
                        mark = ("  [" + ",".join(flags) + "]") if flags else ""
                        print(f"   p{page_num:<4} raw {len(raw):>5}ch  "
                              f"corrected {len(corrected):>5}ch  "
                              f"{ocr_s + corr_s:5.1f}s{mark}")
                    else:
                        skipped += 1

    except KeyboardInterrupt as why:
        interrupted = True
        print(f"\n-- stopped ({why or 'interrupt'}) --")
        print("Everything already written is safe. Run the same command again")
        print("and it will carry on from here rather than starting over.")

    store.finish_run(run_id, written, skipped, flagged, len(pdfs))

    elapsed = time.time() - started
    print(f"\n{'=' * 58}")
    print(f"run {run_id}{' (interrupted)' if interrupted else ''}")
    print(f"  pages written  : {written}")
    print(f"  pages skipped  : {skipped}   (already done on an earlier run)")
    if rejected:
        print(f"  docs rejected  : {rejected}   ({rejected_pages} pages) "
              f"-- born-digital, not scans")
    print(f"  pages flagged  : {flagged}"
          + (f"   ({100 * flagged / written:.0f}% of written)" if written else ""))
    print(f"  elapsed        : {elapsed / 60:.1f} min")
    if written:
        print(f"  per page       : {elapsed / written:.1f}s")

    show_status(store)
    return 0


def show_status(store: Store = None):
    store = store or Store()
    s = store.summary()
    print(f"\n{'=' * 58}")
    print("DATASET SO FAR")
    print(f"  documents      : {s['documents']}")
    print(f"  pages          : {s['pages']}")
    print(f"  flagged        : {s['flagged']}")
    for flag, count in sorted(s["by_flag"].items(), key=lambda kv: -kv[1]):
        print(f"      {flag:<28} {count}")
    if s.get("by_kind"):
        print("  source documents:")
        for kind, count in sorted(s["by_kind"].items()):
            print(f"      {kind:<28} {count}")
    print(f"  runs recorded  : {s['runs']}")
    print(f"  pairs file     : {s['jsonl']}")
    print(f"  database       : {s['db']}")


def main():
    p = argparse.ArgumentParser(
        description="Generate (raw OCR, corrected) pairs from scanned PDFs.",
    )
    p.add_argument("--input", default=DEFAULT_INBOX,
                   help="folder of PDFs to process (searched recursively)")
    p.add_argument("--corrector", default="none", choices=["none", "lighton"],
                   help="'none' runs Tesseract only and flags every row "
                        "no_corrector; use it to test the pipeline without a GPU")
    p.add_argument("--adapter", default=None,
                   help="LoRA folder from train_lighton_ocr.py. Omit to use the "
                        "untuned base model -- useful for measurement, but the "
                        "thesis ships the fine-tune")
    p.add_argument("--image-edge", type=int, default=1536,
                   help="longest image edge given to the corrector; must match "
                        "what it was fine-tuned at")
    p.add_argument("--lang", default="sin", help="Tesseract language")
    p.add_argument("--limit", type=int, default=0,
                   help="stop after N pages this run (0 = no limit)")
    p.add_argument("--note", default=None, help="free text stored with the run")
    p.add_argument("--allow-digital", action="store_true",
                   help="process born-digital PDFs too. OFF by default: OCRing "
                        "a rendered picture of clean typeset text is synthetic "
                        "degradation, which this project forbids in Pipeline B")
    p.add_argument("--status", action="store_true",
                   help="print what has been generated so far and exit")
    args = p.parse_args()

    if args.status:
        show_status()
        return 0
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
