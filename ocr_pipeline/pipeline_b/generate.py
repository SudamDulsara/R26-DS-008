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
import re
import subprocess
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


#: Sinhala legal text is dense with numbers that ARE the content -- Act
#: numbers, years, section and subsection references. The ByT5 corrector was
#: trained on lines whose leading token is nearly always a clause marker
#: ("(1)", "(අ)"), and years at the start of a line were rare, so it rewrites
#: one into the other: "1984 අංක 69 දරන" comes back as "(1) අංක 69 දරන".
#: Measured at 30% of changed lines on the first two pages generated.
#:
#: Every other guard here is blind to it. The length is right, the script is
#: right, there is no markup and no repetition loop -- the page scores
#: flags: (none) while its numbers are being quietly rewritten. Of the
#: failure modes seen on this project that is the dangerous shape: LightOnOCR
#: emitting Kannada is obvious, this is not.
#:
#: The project rule is that when correction makes something worse, the row is
#: flagged and the rate reported per run. This is the flag; main() prints the
#: rate.
_DIGITS = re.compile("[0-9]+")


def digit_line_stats(raw: str, corrected: str) -> tuple:
    """
    Return (lines_changed, lines_whose_digits_changed).

    Compared by position, which is exact here and only here: the corrector
    works line by line and puts the page back together with the same line
    breaks, so the two sides cannot drift. Page-level comparison between two
    different readers needs real sequence alignment -- see build_line_pairs.py
    -- because there the line counts disagree.
    """
    changed = digits = 0
    for a, b in zip(raw.split("\n"), corrected.split("\n")):
        if not a.strip() or a == b:
            continue
        changed += 1
        if _DIGITS.findall(a) != _DIGITS.findall(b):
            digits += 1
    return changed, digits


def quality_flags(raw: str, corrected: str, corrector_name: str,
                  hit_cap: bool, page_num: int = 0) -> list:
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

    # Page 1 of an Act is a cover page -- coat of arms, centred title, date,
    # printing notice. Nothing in the training data looks like it, so both
    # correctors misbehave there and in different ways: LightOnOCR returned
    # Kannada wrapped in HTML, ByT5 rewrites the year into a clause marker.
    # Flagging is the right response to input a model was never taught, and
    # it lets anyone using the dataset drop these rows in one filter.
    if page_num == 1:
        flags.append("cover_page")

    if not raw.strip():
        flags.append("empty_raw")
    if not corrected.strip():
        flags.append("empty_corrected")

    _, digits_changed = digit_line_stats(raw, corrected)
    if digits_changed:
        flags.append("digits_changed")

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
    #
    # TWO thresholds. The first version had only the >20-character rule and
    # missed a real loop on 2026-08-21: page 2 of a 1982 Act ended in a
    # nine-character line repeated to the token cap, so every occurrence was
    # filtered out before counting.
    #
    # A long line repeating four times is suspicious. A short line repeating
    # ten times is a loop -- no page of legal prose repeats the same short
    # line ten times.
    stripped = [ln.strip() for ln in corrected.split(chr(10)) if ln.strip()]
    long_lines = [ln for ln in stripped if len(ln) > 20]
    if long_lines and Counter(long_lines).most_common(1)[0][1] >= 4:
        flags.append("repetition")
    elif stripped and Counter(stripped).most_common(1)[0][1] >= 10:
        flags.append("repetition")

    # ---- has the model stopped being the fine-tune? ----
    #
    # Both checks below catch the same underlying event: a page unlike
    # anything in the 707 training pages, on which the model abandons what it
    # was taught and falls back to the habits of the base checkpoint.
    #
    # Observed 2026-08-18 on the cover page of a 1982 Act -- coat of arms,
    # centred title, a date, a printing notice. Tesseract read 499 characters
    # of ordinary Sinhala from it. The fine-tuned model returned KANNADA
    # wrapped in HTML: "<div style=\"text-align: center;\"> <h2>...".
    #
    # This is not hypothetical damage. A published post-OCR correction dataset
    # whose corrected column contains a different language is worse than one
    # with errors in it, because a downstream model would learn the
    # substitution as if it were a correction.
    #
    # Neither check can fire on a page the model handled properly: correct
    # output is Sinhala, and it is plain text.

    if corrected.strip():
        # Script drift. Compare like with like -- count only letters, so
        # digits, punctuation and the clause markers common to both scripts
        # cannot mask the change.
        sinhala = sum(0x0D80 <= ord(ch) <= 0x0DFF for ch in corrected)
        letters = sum(ch.isalpha() for ch in corrected)
        raw_sinhala = sum(0x0D80 <= ord(ch) <= 0x0DFF for ch in raw)
        if letters >= 40 and raw_sinhala >= 40:
            if sinhala / letters < SINHALA_LETTER_FLOOR:
                flags.append("wrong_script")

        # Markup leakage. The base checkpoint is trained to emit documents as
        # HTML/markdown; the fine-tune was trained on plain page text and
        # never produces these.
        low = corrected.lower()
        if any(tag in low for tag in ("<div", "<h1>", "<h2>", "<table",
                                      "<p>", "![image", "<hr />")):
            flags.append("markup")

    return flags


#: Below this share of Sinhala letters, the corrected side is not Sinhala.
#:
#: Deliberately low. A correct page is essentially 100% Sinhala letters, and
#: the Kannada page measured 0%, so anything between the two separates them.
#: A loose threshold avoids flagging a legitimate page that happens to carry
#: an English schedule heading or a Latin-script citation.
SINHALA_LETTER_FLOOR = 0.50

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


def open_store(args) -> Store:
    """
    Build the Store, honouring --data-dir.

    Why --data-dir exists: resumption is keyed on (doc_id, page_num), so a
    document already in the database is SKIPPED on the next run even if you
    have since improved the corrector. That is correct for the real dataset --
    it is what makes the pipeline continuous -- but it makes the store awkward
    to experiment against, because a trial run silently does nothing.

    Pointing --data-dir at a scratch folder gives a throwaway database to test
    against, leaving the real one untouched. Deleting the real database to
    re-run a test would work too, and is the wrong habit to build.
    """
    if not args.data_dir:
        return Store()
    return Store(data_dir=args.data_dir,
                 support_dir=os.path.join(args.data_dir, "generated"))


def run(args) -> int:
    # PyMuPDF, imported late so --status needs no deps.
    #
    # The package renamed its module from `fitz` to `pymupdf`. Recent
    # versions still ship `fitz` but print a deprecation warning, and newer
    # ones drop it entirely -- which is what a fresh `pip install pymupdf`
    # gets you on Kaggle, where this failed with ModuleNotFoundError while
    # the local venv (an older build) was fine. Try the new name first so
    # this works on both.
    try:
        import pymupdf as fitz
    except ImportError:
        import fitz

    store = open_store(args)
    pdfs = iter_pdfs(args.input)
    if not pdfs:
        print(f"No PDFs found in {args.input}")
        print("Put some scanned Act PDFs there and run again.")
        return 1

    print(f"input      : {args.input}")
    print(f"documents  : {len(pdfs)}")
    print(f"corrector  : {args.corrector}"
          + (f" (adapter: {args.adapter})" if args.adapter else "")
          + (f" (model: {args.model})" if args.model else ""))
    print(f"output     : {store.jsonl_path}")
    print(f"             {store.db_path}")

    tesseract = TesseractReader(lang=args.lang)
    print(f"tesseract  : {tesseract.version}")

    corrector = build_corrector(
        args.corrector,
        adapter_dir=args.adapter,
        model_dir=args.model,
        max_image_edge=args.image_edge,
    ) if args.corrector != "none" else build_corrector("none")

    run_id = uuid.uuid4().hex[:12]
    store.start_run(run_id, args.input, corrector.name, note=args.note)
    print(f"run id     : {run_id}\n")

    written = skipped = flagged = 0
    run_lines_changed = run_lines_digits = 0
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

                    flags = quality_flags(raw, corrected, corrector.name,
                                          hit_cap, page_num)
                    n_chg, n_dig = digit_line_stats(raw, corrected)
                    run_lines_changed += n_chg
                    run_lines_digits += n_dig
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
    # The project rule is that a line is never truncated silently -- skip,
    # split or flag, and log every occurrence. ByT5Corrector passes overlong
    # lines through untouched; this is where that gets reported. A large
    # number means the byte window is wrong for this corpus.
    # The rate the project rule asks for. It is a proxy, not an error count:
    # some of these are the corrector FIXING a number (4989 -> 1989 was one).
    # It says how much of the numeric content the corrector touched, which is
    # what a reader of the dataset needs in order to trust it or not.
    if run_lines_changed:
        print(f"  digits altered : {run_lines_digits} of "
              f"{run_lines_changed} corrected lines "
              f"({100 * run_lines_digits / run_lines_changed:.0f}%)")

    too_long = getattr(corrector, "lines_too_long", 0)
    if getattr(corrector, "lines_seen", 0):
        print(f"  lines corrected: {corrector.lines_changed}"
              f" of {corrector.lines_seen}")
        print(f"  lines too long : {too_long}   "
              "(passed through uncorrected, never truncated)")
    print(f"  elapsed        : {elapsed / 60:.1f} min")
    if written:
        print(f"  per page       : {elapsed / written:.1f}s")

    if written and not args.no_lines:
        derive_line_pairs(store)

    show_status(store)
    return 0


def derive_line_pairs(store) -> None:
    """
    Turn the page pairs just written into aligned line pairs.

    Pages are the primary record: they are exactly what the two readers
    produced, and they need no alignment to exist. Lines are DERIVED from
    them, and derived fresh each time rather than appended to, so the line
    file always describes whatever the page database currently holds. Stop a
    run at 40 pages and finish it tomorrow, and the lines regenerate to cover
    all of them.

    Run as a separate process on purpose. By the time this is called every
    page is already safely on disk, so nothing here can put the dataset at
    risk -- and isolating it means a failure in alignment costs a convenience
    file rather than the run. build_line_pairs.py only ever READS the page
    database; it writes to its own file.
    """
    script = os.path.join(HERE, "build_line_pairs.py")
    if not os.path.exists(script):
        return

    lines_db = os.path.splitext(store.db_path)[0] + "_lines.db"
    lines_jsonl = os.path.join(os.path.dirname(store.jsonl_path),
                               "line_pairs.jsonl")

    print(f"\nderiving line pairs -> {lines_db}")
    try:
        proc = subprocess.run(
            [sys.executable, script,
             "--db", store.db_path,
             "--out", lines_jsonl,
             "--sqlite", lines_db],
            capture_output=True, text=True,
            encoding="utf-8", errors="replace",
        )
    except Exception as exc:                     # noqa: BLE001
        print(f"  skipped: {type(exc).__name__}: {exc}")
        print("  the page pairs are unaffected -- run build_line_pairs.py "
              "by hand if you want them")
        return

    if proc.returncode != 0:
        print("  FAILED -- the page pairs are unaffected")
        for line in (proc.stderr or "").strip().splitlines()[-4:]:
            print("   ", line)
        return

    for line in (proc.stdout or "").splitlines():
        t = line.strip()
        if t.startswith(("aligned pairs", "match rate", "identical pairs")):
            print("  " + t)


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
    p.add_argument("--corrector", default="none",
                   choices=["none", "lighton", "byt5"],
                   help="'none' runs Tesseract only and flags every row "
                        "no_corrector; use it to test the pipeline without a GPU")
    p.add_argument("--adapter", default=None,
                   help="LoRA folder from train_lighton_ocr.py. Omit to use the "
                        "untuned base model -- useful for measurement, but the "
                        "thesis ships the fine-tune")
    p.add_argument("--model", default=None,
                   help="folder holding the trained ByT5 (config.json + "
                        "model.safetensors). Required by --corrector byt5")
    p.add_argument("--image-edge", type=int, default=1536,
                   help="longest image edge given to the corrector; must match "
                        "what it was fine-tuned at")
    p.add_argument("--lang", default="sin", help="Tesseract language")
    p.add_argument("--limit", type=int, default=0,
                   help="stop after N pages this run (0 = no limit)")
    p.add_argument("--no-lines", action="store_true",
                   help="skip deriving the line-level database at the end of "
                        "the run; pages are written either way")
    p.add_argument("--note", default=None, help="free text stored with the run")
    p.add_argument("--allow-digital", action="store_true",
                   help="process born-digital PDFs too. OFF by default: OCRing "
                        "a rendered picture of clean typeset text is synthetic "
                        "degradation, which this project forbids in Pipeline B")
    p.add_argument("--data-dir", default=None,
                   help="write the database and JSONL somewhere other than "
                        "data/ . Use a scratch folder to trial a corrector "
                        "without touching the real dataset")
    p.add_argument("--status", action="store_true",
                   help="print what has been generated so far and exit")
    args = p.parse_args()

    if args.status:
        show_status(open_store(args))
        return 0
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
