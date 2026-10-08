"""
Score the dataset: how wrong is the raw column, how right is the corrected one.

The pipeline's corrected column is model output, and on a production Act there
is no gold text to check it against. This closes that where it can be closed:
for documents that appear in an independently human-corrected corpus, every
line gets a measured error rate for BOTH columns.

    cer_raw         Tesseract's error against the reference
    cer_corrected   the model's error against the same reference
    improved        1 when correction moved the line closer to the reference

REFERENCE, NOT GROUND TRUTH. SinhaLegal (Minduli-Lasandi, arXiv 2603.04854) is
Google Document AI output that its authors then corrected by hand. It is far
better than anything this pipeline produces and it was made independently of
it, which is what a reference needs to be. It is not infallible, and nothing
here should be described as ground truth.

WHY THIS IS NOT CIRCULAR. The corrector was trained on 173 SinhaLegal
documents. None of them is in this dataset -- the training build excluded
every Act in the input folder, and that exclusion was verified at zero
overlap. So the reference text for these pages is genuinely held out.

COVERAGE IS PARTIAL AND SAID SO. Only 369 of SinhaLegal's 1,065 Act folders
contain text at all, so roughly half these documents have no reference. Those
rows are recorded as unmeasured rather than guessed at, and the coverage
figure is reported as a first-class number.

Scores live in their own table rather than as columns on `line_pairs`, which
build_line_pairs.py rebuilds from scratch on every pipeline run. Keyed on
(source_file, page_num, line_num), they survive that rebuild; the `scored`
view joins the two back together.
"""
import io
import json
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import sqlite3                                                  # noqa: E402
import jiwer                                                    # noqa: E402
from pipeline.normalize import normalize                        # noqa: E402
from build_line_pairs import align, split_lines                 # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REFERENCE_DIR = os.path.join(HERE, "data", "_reference", "SinhaLegal", "Acts")
REFERENCE_NAME = "sinhalegal"

#: Words that appear in so many Act titles they carry no signal for matching.
STOP = {"amendment", "special", "provisions", "incorporation", "acts", "act",
        "national", "sri", "lanka"}

#: Below this share of a document's lines aligning, treat the reference as the
#: wrong document rather than a hard scan. See the rejection in main().
MIN_ALIGN_COVERAGE = 0.25

SCHEMA = """
DROP TABLE IF EXISTS line_scores;
CREATE TABLE line_scores (
    source_file    TEXT,
    page_num       INTEGER,
    line_num       INTEGER,

    reference      TEXT,     -- which corpus supplied the reference text
    ref_text       TEXT,     -- the reference line itself, for inspection
    similarity     REAL,     -- alignment confidence, not a quality score

    cer_raw        REAL,     -- Tesseract vs reference
    cer_corrected  REAL,     -- the model vs reference
    wer_raw        REAL,
    wer_corrected  REAL,
    improved       INTEGER,  -- 1 when the model moved closer to the reference

    PRIMARY KEY (source_file, page_num, line_num)
);
DROP VIEW IF EXISTS scored;
-- `improved` and `reference` stay in line_scores -- the run summary counts
-- one and the other records where the reference came from -- but neither
-- belongs in the browsing view, which is for reading the comparison itself.
CREATE VIEW scored AS
    SELECT p.source_file, p.page_num, p.line_num,
           p.raw, p.corrected, s.ref_text,
           ROUND(s.cer_raw, 4)       AS cer_raw,
           ROUND(s.cer_corrected, 4) AS cer_corrected
    FROM line_pairs p
    JOIN line_scores s
      ON p.source_file = s.source_file
     AND p.page_num    = s.page_num
     AND p.line_num    = s.line_num
    ORDER BY p.source_file, p.page_num, p.line_num;
"""


def toks(s):
    return {w for w in re.split(r"[^a-z]+", s.lower())
            if len(w) > 3 and w not in STOP}


def act_titles(catalogue_path):
    """{(act_no, year): title} from the cached documents.gov.lk listing."""
    raw = io.open(catalogue_path, encoding="utf-8").read()
    line = next(l for l in raw.split("\n") if l.startswith("1:"))
    return {(a["actNo"], a["actSubNo"]): (a["descriptionEnglish"] or "").strip()
            for a in json.loads(line[2:])["data"]}


def reference_text(act_no, year, titles):
    """The human-corrected text for this Act, or None."""
    title = titles.get((act_no, year))
    folder = os.path.join(REFERENCE_DIR, str(year))
    if not title or not os.path.isdir(folder):
        return None
    want = toks(title)
    if not want:
        return None
    best, score = None, 0.0
    for name in os.listdir(folder):
        s = len(want & toks(name)) / len(want)
        if s > score:
            best, score = name, s
    if not best or score < 0.5:
        return None
    d = os.path.join(folder, best)
    txt = [f for f in os.listdir(d) if f.endswith(".txt")]
    if not txt:
        return None                       # folder exists, holds metadata only
    return normalize(io.open(os.path.join(d, txt[0]), encoding="utf-8").read())


def cer(ref, hyp):
    """Both sides whitespace-collapsed: we are scoring characters, not layout."""
    r, h = " ".join(ref.split()), " ".join(hyp.split())
    if not r:
        return None
    return jiwer.cer(r, h)


def wer(ref, hyp):
    r, h = " ".join(ref.split()), " ".join(hyp.split())
    if not r:
        return None
    return jiwer.wer(r, h)


def main():
    root = os.path.dirname(HERE)
    lines_db = os.environ.get(
        "LINES_DB", os.path.join(root, "data", "inbox",
                                 "ocr_correction_pairs_lines.db"))
    catalogue = os.environ.get(
        "CATALOGUE", os.path.join(root, "data", "_acts_catalogue.txt"))

    if not os.path.exists(lines_db):
        sys.exit(f"no line database at {lines_db}")
    if not os.path.isdir(REFERENCE_DIR):
        sys.exit(f"no reference corpus at {REFERENCE_DIR}")
    if not os.path.exists(catalogue):
        sys.exit(f"no Act catalogue at {catalogue}. Run the pipeline with "
                 "--fetch once, or copy the cached listing there.")

    titles = act_titles(catalogue)
    db = sqlite3.connect(lines_db)
    db.executescript(SCHEMA)

    docs = [r[0] for r in db.execute(
        "SELECT DISTINCT source_file FROM line_pairs ORDER BY source_file")]
    print(f"documents in the dataset : {len(docs)}\n")

    written = 0
    no_reference = []
    per_doc = []

    for src in docs:
        m = re.match(r"(\d+)-(\d{4})_", os.path.basename(src))
        if not m:
            no_reference.append((src, "unparseable name"))
            continue
        act_no, year = int(m.group(1)), int(m.group(2))

        ref = reference_text(act_no, year, titles)
        if ref is None:
            no_reference.append((f"{act_no}/{year}", "no text in reference corpus"))
            continue

        rows = list(db.execute(
            "SELECT page_num, line_num, raw, corrected FROM line_pairs "
            "WHERE source_file=? ORDER BY page_num, line_num", (src,)))
        raw_lines = [r[2] for r in rows]
        ref_lines = split_lines(ref)

        # Align the document's OCR lines to the reference's lines. The two
        # sides disagree on count -- running headers repeat on every scanned
        # page and appear once in the reference -- so unmatched lines are left
        # unscored rather than paired with whatever is nearest.
        pairs, _, _ = align(raw_lines, ref_lines)

        doc_rows, cr, cc = [], [], []
        for i, j, sim in pairs:
            page_num, line_num, raw, corrected = rows[i]
            r = ref_lines[j]
            a, b = cer(r, raw), cer(r, corrected)
            if a is None or b is None:
                continue
            doc_rows.append((src, page_num, line_num, REFERENCE_NAME, r, sim,
                             a, b, wer(r, raw), wer(r, corrected),
                             int(b < a)))
            cr.append(a)
            cc.append(b)

        # REJECT A DOCUMENT THAT BARELY ALIGNS. Acts share a great deal of
        # boilerplate -- every one opens with the same republic and parliament
        # formula -- so a title lookup that finds the WRONG Act still produces
        # a handful of confident-looking matches on that shared text. Act
        # 26/1988 did exactly this: 3% of its lines aligned against a
        # different document, and those rows would have entered the dataset
        # as measurements.
        #
        # Genuine matches here run 44-84%. Anything under a quarter is a
        # mismatched reference, not a hard scan, and its scores are worse than
        # no scores because they look real.
        coverage = len(doc_rows) / max(1, len(rows))
        if not doc_rows or coverage < MIN_ALIGN_COVERAGE:
            no_reference.append((
                f"{act_no}/{year}",
                f"only {100 * coverage:.0f}% aligned -- wrong reference, rejected"))
            continue

        db.executemany(
            "INSERT OR REPLACE INTO line_scores VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            doc_rows)
        written += len(doc_rows)
        per_doc.append((f"{act_no}/{year}", len(doc_rows), len(rows),
                        statistics.mean(cr), statistics.mean(cc)))
        print(f"  {act_no:>3}/{year}  {len(doc_rows):4} of {len(rows):4} lines "
              f"scored   raw {statistics.mean(cr):.4f} -> "
              f"corrected {statistics.mean(cc):.4f}")

    db.commit()

    total_lines = db.execute("SELECT COUNT(*) FROM line_pairs").fetchone()[0]
    rows = list(db.execute(
        "SELECT cer_raw, cer_corrected, improved FROM line_scores"))
    print(f"\n{'=' * 66}")
    if not rows:
        print("  nothing could be scored.")
        return 1

    cr = [r[0] for r in rows]
    cc = [r[1] for r in rows]
    better = sum(r[2] for r in rows)
    print(f"  lines scored           : {len(rows):,} of {total_lines:,} "
          f"({100 * len(rows) / total_lines:.0f}% coverage)")
    print(f"  documents scored       : {len(per_doc)} of {len(docs)}")
    print()
    print(f"  RAW (Tesseract)        : {statistics.mean(cr):.4f} CER   "
          f"median {statistics.median(cr):.4f}")
    print(f"  CORRECTED (the model)  : {statistics.mean(cc):.4f} CER   "
          f"median {statistics.median(cc):.4f}")
    drop = 100 * (statistics.mean(cr) - statistics.mean(cc)) / statistics.mean(cr)
    print(f"  change                 : {drop:+.1f}% "
          f"{'fewer' if drop > 0 else 'MORE'} character errors")
    print()
    print(f"  lines improved         : {better:,} ({100*better/len(rows):.0f}%)")
    print(f"  lines made worse       : {len(rows)-better:,} "
          f"({100*(len(rows)-better)/len(rows):.0f}%)")

    if no_reference:
        print(f"\n  UNMEASURED -- no reference available ({len(no_reference)} documents)")
        for tag, why in no_reference[:14]:
            print(f"      {tag:12} {why}")
        print("    These rows carry no score. Not estimated, not guessed.")

    print(f"\n  written to {lines_db}")
    print("    table `line_scores`, view `scored` (joins the pairs to their scores)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
