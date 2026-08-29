"""
Turn Pipeline B's PAGE pairs into LINE pairs that ByT5 can actually train on.

WHY THIS EXISTS
---------------
Pipeline B writes one row per page: Tesseract's reading of the whole page on
one side, the fine-tuned OCR model's reading of the same page on the other.
That is the right primary record -- it is lossless and needs no alignment to
produce.

But ByT5 is a byte-level model reading MAX_LENGTH (384) bytes at a time, and
an acts-1010 page is ~1,600 characters, which is ~4,800 bytes in Sinhala
UTF-8. Feeding it a page truncates 92% of it. Measured line geometry over the
202 test pages: mean 105 bytes, median 107, 99th percentile 185, longest 218
-- so LINES fit the window with room to spare and pages never will.

WHY ALIGNMENT IS NEEDED, RATHER THAN zip()
------------------------------------------
The two sides do not have the same number of lines. Tesseract sometimes
splits one printed line in two (observed 37/37, 36/37, 38/44 against the gold
text), and the corrector re-reads the image independently, so it makes its
own line-break decisions. Pairing by position would silently misalign
everything after the first disagreement, and every pair past that point would
teach the model to turn one line into a different line. Nothing would look
wrong; the CER would just never improve.

So this aligns the two line lists properly, using Needleman-Wunsch over
pairwise similarity. Lines that cannot be matched confidently are dropped and
counted rather than guessed at.

USAGE
-----
    python build_line_pairs.py                        # reads the default DB
    python build_line_pairs.py --db path/to.db --out data/line_pairs/pairs.jsonl
    python build_line_pairs.py --jsonl data/generated/pairs.jsonl

Output is JSONL with `raw` and `corrected` fields, which is what
train_byt5.py's _read_pair() understands, plus provenance so any pair can be
traced back to its page.

DO NOT put the output in a folder whose name starts with an underscore.
train_byt5.py skips those -- that is this project's convention for retired
data -- and your training run would silently find nothing.
"""

import argparse
import json
import os
import sqlite3
import statistics
import sys
from collections import Counter
from difflib import SequenceMatcher

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pipeline.normalize import normalize          # noqa: E402


# ══════════════════════════════════════════════════════════════
#  Settings
# ══════════════════════════════════════════════════════════════

#: Flags that disqualify a whole page.
#:
#: These are not "suspicious", they are known-bad, and one of them poisons
#: every line it contributes:
#:
#:   no_corrector      the corrected column is a COPY of the raw column, so
#:                     every pair would teach "change nothing"
#:   empty_*           nothing to align
#:   wrong_script      the model fell out of distribution and emitted another
#:                     language -- observed on cover pages, which returned
#:                     Kannada
#:   markup            it emitted HTML instead of text, same cause
#:   repetition        it degenerated into a loop
#:   hit_token_cap     generation was cut off, so the tail of the page is
#:                     missing and the alignment there is meaningless
#:
#: `identical`, `much_shorter` and `much_longer` are deliberately NOT here.
#: The first is handled by train_byt5.py's own filter; the other two are soft
#: signals, and the aligner drops whatever it cannot match anyway.
BAD_FLAGS = {
    "no_corrector", "empty_raw", "empty_corrected",
    "wrong_script", "markup", "repetition", "hit_token_cap",
}

#: Below this similarity, two lines are probably not the same line.
#:
#: Pairing lines that are not really the same line is the failure mode this
#: whole script exists to prevent, and it is invisible in the output -- you
#: get a plausible JSONL full of nonsense. 0.45 roughly corresponds to the
#: CER 0.6 cutoff train_byt5.py already applies, so this errs the same way.
MIN_SIMILARITY = 0.45

#: Needleman-Wunsch gap penalty. Leaving a line unmatched costs this much,
#: so a merge or a split loses one line instead of forcing a bad pair.
GAP_PENALTY = 0.45

#: ByT5's window. Pairs longer than this are reported, not dropped -- knowing
#: how many there are tells you whether the corrector is producing runaway
#: lines, which page-level flags would not catch.
MAX_BYTES = 384


# ══════════════════════════════════════════════════════════════
#  Alignment
# ══════════════════════════════════════════════════════════════

def similarity(a: str, b: str) -> float:
    """0..1 overlap between two lines. 1.0 means identical."""
    if a == b:
        return 1.0
    # real_quick_ratio is a cheap upper bound; skip the real work when even
    # the optimistic estimate is hopeless. Pages have ~45 lines each, so this
    # runs ~2,000 times per page and the shortcut matters.
    sm = SequenceMatcher(None, a, b, autojunk=False)
    if sm.real_quick_ratio() < MIN_SIMILARITY:
        return 0.0
    if sm.quick_ratio() < MIN_SIMILARITY:
        return 0.0
    return sm.ratio()


def align(raw_lines, cor_lines):
    """
    Match raw lines to corrected lines, in order, allowing gaps on both sides.

    Needleman-Wunsch: build a table where cell (i, j) holds the best score for
    aligning the first i raw lines against the first j corrected lines, then
    walk back through it to recover the choices that produced that score.

    Order is preserved, which is the property that makes this safe -- a line
    can never be matched to one that came earlier on the other side.

    Returns (pairs, unmatched_raw, unmatched_corrected) where pairs is a list
    of (raw_index, corrected_index, similarity).
    """
    n, m = len(raw_lines), len(cor_lines)
    if not n or not m:
        return [], n, m

    # score[i][j] and a parallel table of moves: 0 = pair, 1 = skip raw,
    # 2 = skip corrected.
    score = [[0.0] * (m + 1) for _ in range(n + 1)]
    move = [[0] * (m + 1) for _ in range(n + 1)]

    for i in range(1, n + 1):
        score[i][0] = score[i - 1][0] - GAP_PENALTY
        move[i][0] = 1
    for j in range(1, m + 1):
        score[0][j] = score[0][j - 1] - GAP_PENALTY
        move[0][j] = 2

    for i in range(1, n + 1):
        ri = raw_lines[i - 1]
        for j in range(1, m + 1):
            s = similarity(ri, cor_lines[j - 1])
            diag = score[i - 1][j - 1] + s
            up = score[i - 1][j] - GAP_PENALTY
            left = score[i][j - 1] - GAP_PENALTY
            best = max(diag, up, left)
            score[i][j] = best
            move[i][j] = 0 if best == diag else (1 if best == up else 2)

    pairs, skipped_raw, skipped_cor = [], 0, 0
    i, j = n, m
    while i > 0 or j > 0:
        if i > 0 and j > 0 and move[i][j] == 0:
            s = similarity(raw_lines[i - 1], cor_lines[j - 1])
            if s >= MIN_SIMILARITY:
                pairs.append((i - 1, j - 1, s))
            else:
                # The table paired them because that beat two gaps, but they
                # are not really the same line. Drop rather than teach it.
                skipped_raw += 1
                skipped_cor += 1
            i, j = i - 1, j - 1
        elif i > 0 and (j == 0 or move[i][j] == 1):
            skipped_raw += 1
            i -= 1
        else:
            skipped_cor += 1
            j -= 1

    pairs.reverse()
    return pairs, skipped_raw, skipped_cor


# ══════════════════════════════════════════════════════════════
#  Input
# ══════════════════════════════════════════════════════════════

def split_lines(text: str):
    """Normalised, non-empty lines. Normalisation happens ONCE, here."""
    return [ln.strip() for ln in normalize(text).split("\n") if ln.strip()]


def rows_from_db(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    for r in db.execute(
        "SELECT doc_id, source_file, page_num, raw_text, corrected_text, flags "
        "FROM pages ORDER BY source_file, page_num"
    ):
        yield dict(r)
    db.close()


def rows_from_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            yield {
                "doc_id": r.get("doc_id"),
                "source_file": r.get("source_file"),
                "page_num": r.get("page_num"),
                "raw_text": r.get("raw", ""),
                "corrected_text": r.get("corrected", ""),
                "flags": r.get("flags") or "",
            }


# ══════════════════════════════════════════════════════════════
#  Overlap guard: never train on a page the exam already contains
# ══════════════════════════════════════════════════════════════
#
# The corrector was fine-tuned on acts-1010's train split and will be graded
# on its test split. Pipeline input comes from documents.gov.lk -- the same
# source, the same years -- so some input pages ARE acts-1010 pages. Measured
# on the first real run: 13 of 90, three of them from the TEST split.
#
# Two different harms:
#
#   test-split overlap   ByT5 would train on pages it is later graded on.
#                        That invalidates every number, and it is invisible
#                        unless you look for it.
#   train-split overlap  The corrector memorised those pages during
#                        fine-tuning, so its "correction" there is recall.
#                        The dataset looks cleaner than the pipeline is.
#
# Matching is on TEXT, not filenames, because the two sources name documents
# differently -- acts-1010 files are `year_docid_page` with no Act number.
# A cheap token-overlap pass picks candidates, then a real order-sensitive
# ratio decides. Order matters: difflib's quick_ratio() ignores it, and two
# unrelated pages of Sinhala legal prose score ~0.84 on that measure alone.

OVERLAP_THRESHOLD = 0.60


def load_acts1010(path):
    """Return [(split, filename, text, token set)] or None if not on disk."""
    if not os.path.isdir(path):
        return None
    try:
        from datasets import load_dataset
    except ImportError:
        return None
    out = []
    for split in ("train", "eval", "test"):
        ds = load_dataset(path, split=split)
        for i in range(len(ds)):
            t = normalize(ds[i]["text"])
            out.append((split, ds[i]["filename"], t, set(t.split())))
    return out


def overlap_of(text, corpus):
    """(split, filename, ratio) of the closest corpus page, or None."""
    toks = set(text.split())
    if len(toks) < 20:
        return None
    scored = sorted(corpus,
                    key=lambda c: -len(toks & c[3]) / max(1, len(toks | c[3])))
    best = None
    for split, name, ctext, _ in scored[:3]:
        r = SequenceMatcher(None, text, ctext, autojunk=False).ratio()
        if best is None or r > best[2]:
            best = (split, name, r)
    return best if best and best[2] >= OVERLAP_THRESHOLD else None


# ══════════════════════════════════════════════════════════════
#  Optional SQLite output, for reading the pairs by eye
# ══════════════════════════════════════════════════════════════
#
# The JSONL is what train_byt5.py consumes and what gets published. This is
# purely for looking at: a page pair in DB Browser is two 2,000-character
# blobs side by side, which nobody can actually compare. One row per LINE,
# with `raw` and `corrected` in adjacent columns, is readable at a glance --
# and it is what to put in front of a supervisor or a panel.
#
# Rebuilt from scratch on every run, so re-running never duplicates rows.

LINE_SCHEMA = """
DROP TABLE IF EXISTS line_pairs;
CREATE TABLE line_pairs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file     TEXT,
    page_num        INTEGER,
    line_num        INTEGER,

    -- kept adjacent on purpose: this is the comparison
    raw             TEXT,
    corrected       TEXT,

    changed         INTEGER,   -- 1 when the corrector altered the line
    similarity      REAL,      -- 1.0 = identical, floor is MIN_SIMILARITY
    raw_chars       INTEGER,
    corrected_chars INTEGER,
    doc_id          TEXT,
    corrected_is    TEXT       -- the standing "not human-verified" label
);
DROP VIEW IF EXISTS corrections;
CREATE VIEW corrections AS
    SELECT source_file, page_num, line_num, raw, corrected, similarity
    FROM line_pairs WHERE changed = 1
    ORDER BY source_file, page_num, line_num;
"""


def write_sqlite(rows, path):
    """Write the line pairs to a browsable table plus a `corrections` view."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript(LINE_SCHEMA)
    db.executemany(
        "INSERT INTO line_pairs (source_file, page_num, line_num, raw, "
        "corrected, changed, similarity, raw_chars, corrected_chars, doc_id, "
        "corrected_is) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [(r["source_file"], r["page_num"], r["line_num"], r["raw"],
          r["corrected"], int(r["raw"] != r["corrected"]), r["similarity"],
          len(r["raw"]), len(r["corrected"]), r["doc_id"], r["corrected_is"])
         for r in rows],
    )
    db.commit()
    db.close()


# ══════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════

def main():
    global MIN_SIMILARITY          # must precede any use of the name here

    here = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(here, "data", "ocr_correction_pairs.db")

    p = argparse.ArgumentParser(
        description="Split Pipeline B page pairs into aligned line pairs.")
    p.add_argument("--db", default=None,
                   help=f"pipeline database (default: {default_db})")
    p.add_argument("--jsonl", default=None,
                   help="read pairs.jsonl instead of the database")
    p.add_argument("--out", default=os.path.join(here, "data", "line_pairs",
                                                 "pairs.jsonl"),
                   help="where to write the line pairs")
    p.add_argument("--sqlite", nargs="?", const="AUTO", default=None,
                   metavar="PATH",
                   help="also write a browsable SQLite table (open it in DB "
                        "Browser). Bare --sqlite puts line_pairs.db beside "
                        "--out; give a path to choose your own")
    p.add_argument("--exclude-overlap", action="store_true",
                   help="drop pages that also appear in acts-1010. Use this "
                        "for anything ByT5 will train on -- see the overlap "
                        "guard above")
    p.add_argument("--acts1010", default=os.path.join(here, "data", "acts1010"),
                   help="where the acts-1010 parquet lives, for --exclude-overlap")
    p.add_argument("--keep-flagged", action="store_true",
                   help="do not skip pages carrying a disqualifying flag")
    p.add_argument("--min-similarity", type=float, default=MIN_SIMILARITY)
    args = p.parse_args()

    if args.jsonl:
        source, reader = args.jsonl, rows_from_jsonl
    else:
        source, reader = (args.db or default_db), rows_from_db
    if not os.path.exists(source):
        sys.exit(f"no input at {source!r}. Pass --db or --jsonl.")

    MIN_SIMILARITY = args.min_similarity

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    if os.path.basename(os.path.dirname(os.path.abspath(args.out))).startswith("_"):
        print("WARNING: the output folder starts with an underscore, which "
              "train_byt5.py skips. Your training run will find nothing.")

    print(f"reading {source}")

    corpus = None
    if args.exclude_overlap:
        corpus = load_acts1010(args.acts1010)
        if corpus is None:
            sys.exit(f"--exclude-overlap needs acts-1010 at {args.acts1010!r} "
                     "(and the `datasets` package). Point --acts1010 at it.")
        print(f"overlap guard: comparing against {len(corpus):,} acts-1010 pages")

    overlap_hits = Counter()
    overlap_rows = []
    pages = kept_pages = skipped_pages = 0
    #: Counts flag OCCURRENCES, which is not the same as pages: one page can
    #: carry several disqualifying flags at once, and most flagged pages here
    #: do. Track the page count separately or the report over-states it --
    #: 48 occurrences across 26 pages, on the first real run.
    skipped_by_flag = Counter()
    out_rows = []
    total_raw_lines = total_cor_lines = 0
    unmatched_raw = unmatched_cor = 0
    identical = 0

    for r in reader(source):
        pages += 1
        flags = {f.strip() for f in (r["flags"] or "").split(",") if f.strip()}
        bad = flags & BAD_FLAGS
        if bad and not args.keep_flagged:
            skipped_pages += 1
            for f in bad:
                skipped_by_flag[f] += 1
            continue

        raw_lines = split_lines(r["raw_text"] or "")

        if corpus is not None:
            hit = overlap_of(" ".join(raw_lines), corpus)
            if hit:
                split_name, fname, ratio = hit
                overlap_hits[split_name] += 1
                overlap_rows.append((split_name, r["source_file"],
                                     r["page_num"], ratio, fname))
                continue

        cor_lines = split_lines(r["corrected_text"] or "")
        total_raw_lines += len(raw_lines)
        total_cor_lines += len(cor_lines)

        pairs, sr, sc = align(raw_lines, cor_lines)
        unmatched_raw += sr
        unmatched_cor += sc
        kept_pages += 1

        for n, (i, j, sim) in enumerate(pairs, 1):
            if raw_lines[i] == cor_lines[j]:
                identical += 1
            out_rows.append({
                "doc_id": r["doc_id"],
                "source_file": r["source_file"],
                "page_num": r["page_num"],
                "line_num": n,
                "raw": raw_lines[i],
                "corrected": cor_lines[j],
                "similarity": round(sim, 4),
                "corrected_is": "machine-generated, not human-verified",
            })

    with open(args.out, "w", encoding="utf-8") as f:
        for row in out_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    sqlite_path = None
    if args.sqlite and out_rows:
        sqlite_path = (os.path.join(os.path.dirname(os.path.abspath(args.out)),
                                    "line_pairs.db")
                       if args.sqlite == "AUTO" else args.sqlite)
        write_sqlite(out_rows, sqlite_path)

    # ── report ────────────────────────────────────────────────
    print(f"\npages read           : {pages}")
    if skipped_pages:
        print(f"pages skipped        : {skipped_pages}")
        print(f"  (flag occurrences, not pages -- most flagged pages carry "
              f"more than one)")
        for flag, n in skipped_by_flag.most_common():
            print(f"    {flag:<16} {n}")
    if overlap_rows:
        print(f"pages dropped as acts-1010 overlap: {len(overlap_rows)}")
        for split_name in ("test", "eval", "train"):
            n = overlap_hits.get(split_name, 0)
            if not n:
                continue
            note = "  <-- would have contaminated the evaluation" if split_name == "test" else ""
            print(f"    {split_name:<6} {n}{note}")
        for split_name, src, pg, ratio, fname in sorted(overlap_rows,
                                                        key=lambda x: -x[3])[:10]:
            print(f"      {src[:26]:<28} p{pg:<3} {ratio:.3f}  "
                  f"= acts-1010 {split_name}/{fname}")
    print(f"pages used           : {kept_pages}")
    if not kept_pages:
        print("\nNothing was usable. If every page was skipped as "
              "'no_corrector', the pipeline ran in Tesseract-only mode and "
              "the corrected column is a copy of the raw one -- re-run "
              "pipeline_b.generate with --corrector lighton on a GPU.")
        return 1

    print(f"\nlines: {total_raw_lines:,} raw / {total_cor_lines:,} corrected")
    print(f"  aligned pairs      : {len(out_rows):,}")
    print(f"  unmatched raw      : {unmatched_raw:,}")
    print(f"  unmatched corrected: {unmatched_cor:,}")
    match_rate = 100 * len(out_rows) / max(1, total_raw_lines)
    print(f"  match rate         : {match_rate:.0f}% of raw lines")
    if match_rate < 70:
        print("  WARNING: a low match rate means the two sides disagree about "
              "where the lines are. Read some pairs before training on them.")

    if out_rows:
        sims = [r["similarity"] for r in out_rows]
        lens = [len(r["raw"].encode("utf-8")) for r in out_rows]
        over = sum(1 for L in lens if L > MAX_BYTES)
        print(f"\nsimilarity: median {statistics.median(sims):.3f}  "
              f"min {min(sims):.3f}")
        print(f"raw bytes : median {statistics.median(lens):.0f}  "
              f"max {max(lens)}  over {MAX_BYTES}: {over}")
        print(f"identical pairs (no signal, train_byt5 drops these): "
              f"{identical:,} ({100 * identical / len(out_rows):.0f}%)")
        print(f"  -> roughly {len(out_rows) - identical:,} pairs carry a "
              f"correction to learn from")

    print(f"\nwritten to {args.out}")
    if sqlite_path:
        print(f"browsable copy: {sqlite_path}")
        print("  open it in DB Browser -> Browse Data -> table `line_pairs`,")
        print("  or the `corrections` view for only the lines that changed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
