"""
Fine-tune ByT5 for Sinhala post-OCR correction
===============================================
Runs on Kaggle's free T4 GPU. Each numbered section below is one notebook
cell — paste them in order, or run the file as a script.

    Kaggle → New Notebook → Settings → Accelerator: GPU T4 x2
    (phone verification is required before Kaggle offers a GPU)

INPUT  : gazette_pairs.jsonl + ocr_pairs.jsonl  (upload as a Kaggle Dataset)
         test_set.jsonl  — upload this too. It is held out, never trained
         on, and it is the only file that can answer "did this help".
OUTPUT : a model folder you download and drop into the pipeline


WHY ByT5 AND NOT A NORMAL MODEL
-------------------------------
Most models chop text into word-pieces using a fixed vocabulary. Sinhala
breaks that: a letter is a base consonant plus attached vowel signs plus
sometimes an invisible joiner, and word-piece splitters cut through those
inconsistently. ByT5 works on raw bytes, so every diacritic and every
U+200C survives — which matters here, since this project has already
measured 13,500 spurious invisible characters in Tesseract's output.

Precedent: ByT5-Sanskrit (arXiv 2409.13920) cut CER and WER by 23% on
post-OCR correction for Devanagari, another Indic script with the same
conjunct-and-diacritic structure.


THE ONE NUMBER TO BEAT
----------------------
Tesseract scores 0.1079 CER on the 202 human-transcribed acts-1010 test
pages -- the only gold text in this project. Anything above that means the
model is making the text worse.

The old 0.0921 figure came from a retired synthetic test set and is void.
Do not compare against it, and do not compare a line-level score against a
page-level one: they are different denominators.

INPUT MUST BE LINE-LEVEL
------------------------
This is a byte-level model reading MAX_LENGTH bytes at a time. A whole
acts-1010 page is ~1,600 characters, which is ~4,800 bytes in Sinhala UTF-8
-- twelve times the window. Pipeline B writes PAGE pairs, so they must be
split into lines before they reach this script. Section 3 refuses to train
on over-long pairs rather than truncating them, because truncation here
produces a plausible-looking number from mutilated data.
"""

# ══════════════════════════════════════════════════════════════
#  1. Install
# ══════════════════════════════════════════════════════════════
# Kaggle ships transformers, datasets, accelerate and sentencepiece already.
# Only jiwer is missing, and only that is installed here — reinstalling the
# others can pull a different version and force a kernel restart mid-run.

import importlib.util
import subprocess
import sys

for _pkg in ("jiwer",):
    if importlib.util.find_spec(_pkg) is None:
        print(f"installing {_pkg} ...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", _pkg], check=True)

import glob
import json
import math
import os
import random

# ── Both of these must be set before torch is imported ──────────────
#
# One GPU, not two. Kaggle offers T4 x2, and the Trainer then wraps the
# model in DataParallel — which replicates it on both cards and gathers
# every output back onto card 0. Card 0 therefore carries its own share
# plus all the gathered results, and it is the card that ran out. A
# single card uses less memory in total and is predictable.
os.environ["CUDA_VISIBLE_DEVICES"] = "0"

# The failure reported 1.62 GB "reserved but unallocated" — memory held
# by the allocator in pieces too small to reuse. This lets it grow
# segments instead of fragmenting them.
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import numpy as np
import torch
from datasets import Dataset
from jiwer import cer, wer
from transformers import (
    AutoTokenizer,
    AutoModelForSeq2SeqLM,
    DataCollatorForSeq2Seq,
    Seq2SeqTrainer,
    Seq2SeqTrainingArguments,
)

# ══════════════════════════════════════════════════════════════
#  2. Settings
# ══════════════════════════════════════════════════════════════

MODEL_NAME = "google/byt5-small"


#: Any file whose path contains one of these is the evaluation set, never
#: training data. test_set.jsonl uses the same "ocr"/"gold" field names as
#: the training files, so a plain *.jsonl sweep would swallow it silently —
#: the model would then be scored on pages it had already been trained on
#: and every number in the thesis would be meaningless. Splitting by path
#: means the eval set can safely sit in the same uploaded Kaggle dataset.
EVAL_MARKERS = ("test", "eval")


def _in_retired_folder(path: str) -> bool:
    """
    True if any FOLDER in the path starts with an underscore.

    data/ carries live corpora next to retired ones, and the sweep below is
    recursive, so without this it silently trains on both. As of 2026-08-23
    that meant 12.8 MB of the synthetic gazette pairs this project retired in
    August, plus 90 rows whose corrected side is a copy of the raw side --
    every resulting number would have been meaningless, and nothing would
    have looked wrong.

    Underscore-prefixed folders are this project's convention for "kept for
    reference, not part of the live path": data/_synthetic_degradation,
    data/_trial. Naming them was not enough on its own; the sweep had to
    honour the convention too.

    FOLDERS only, deliberately. data/acts1010/_tesseract_test_split.jsonl is
    a leading-underscore FILE holding real Tesseract output on the held-out
    split, and it is legitimate evaluation data.
    """
    parts = path.replace("\\", "/").split("/")[:-1]
    return any(part.startswith("_") for part in parts)



def find_data_files():
    """
    Locate the .jsonl pairs wherever this happens to be running, and split
    them into training files and evaluation files.

    Kaggle mounts an attached dataset at /kaggle/input/<whatever-you-named-it>/,
    so hardcoding a path means editing the script every time the dataset is
    renamed or re-attached. Searching instead keeps the file paste-and-run.
    """
    found = sorted(glob.glob("/kaggle/input/**/*.jsonl", recursive=True))
    if not found:
        found = sorted(glob.glob(os.path.join("data", "**", "*.jsonl"),
                                 recursive=True))
        found = [p for p in found if not _in_retired_folder(p)]

    train, evaluation = [], []
    for path in found:
        marker = path.replace("\\", "/").lower()
        (evaluation if any(m in marker for m in EVAL_MARKERS) else train).append(path)
    return train, evaluation


TRAIN_FILES, EVAL_FILES = find_data_files()
OUTPUT_DIR = "byt5-sinhala-ocr"

if not TRAIN_FILES:
    sys.exit("No .jsonl training files found. On Kaggle: right panel -> Add Input.")
print("training files:")
for _p in TRAIN_FILES:
    print(f"  {_p}  ({os.path.getsize(_p) / 1e6:.1f} MB)")

print("evaluation files (held out, never trained on):")
for _p in EVAL_FILES:
    print(f"  {_p}  ({os.path.getsize(_p) / 1e6:.1f} MB)")
if not EVAL_FILES:
    print("  NONE FOUND — section 7 will be skipped. Add test_set.jsonl to the")
    print("  Kaggle dataset to get the number that actually matters.")

# Measured over all 35,354 pairs, in UTF-8 bytes (what a byte-level model
# actually counts): mean 142, median 123, 99.5th percentile 323, longest 412.
# 512 was reserving room for text that does not exist in this corpus, and
# attention memory grows with the square of this number. 384 clears the
# longest example outright, so nothing is truncated.
MAX_LENGTH = 384

BATCH_SIZE = 4          # halved after an out-of-memory at 8
GRAD_ACCUM = 8          # raised to match — still an effective batch of 32
LEARNING_RATE = 1e-4    # T5's own recommended fine-tuning rate; 3e-4 diverged
#: Passes over the training set. Settable so a run can be retuned from a
#: Kaggle cell without editing this file:  os.environ["EPOCHS"] = "8"
#:
#: WHY THE DEFAULT MOVED FROM 2 TO 8 (2026-08-26). Two epochs was chosen for
#: the retired 35,000-pair corpus, where it came to 1,214 optimiser steps.
#: The live corpus is Pipeline B's own output -- about 2,400 line pairs -- and
#: at an effective batch of 32 that is only ~75 steps per epoch. Two epochs
#: would be 150 steps in total, which is not enough for a seq2seq model to
#: learn a new task; the run would finish, report a number, and mean nothing.
#:
#: Overfitting is not the risk it looks like at this epoch count, because
#: section 6 sets load_best_model_at_end with metric_for_best_model="cer" --
#: whichever checkpoint scores best on the held-out CER is what gets kept,
#: not whichever came last.
#:
#: fp32 costs ~2x the time per step (see FP16 below), so budget accordingly.
EPOCHS = int(os.environ.get("EPOCHS", "8"))
SEED = 42

# CER to beat. Measured here on the 202 human-transcribed acts-1010 test
# pages and independently reported as 0.1069 by the corpus authors -- two
# separate measurements agreeing to 0.001. Per-page mean, both sides NFC.
#
# Replaced 0.0921 on 2026-08-26. That number came from the retired synthetic
# test set and cannot be compared with anything now in the live path.
TESSERACT_BASELINE = 0.1079

random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

print("GPU:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "NONE — enable it in Settings")


# ══════════════════════════════════════════════════════════════
#  3. Load and split the data
# ══════════════════════════════════════════════════════════════

ZWNJ = "‌"

#: Fraction of trivial pairs to keep. Measured on the gazette corpus, 28.5%
#: of pairs differ only by a spurious invisible character and another 5%
#: only by whitespace. Those are real errors and the model must learn them,
#: but they are one deterministic rule — a few thousand examples teach it as
#: well as ten thousand would. Keeping them all would spend a third of
#: training capacity on the easiest thing in the dataset while the genuine
#: character confusions (ව/ච, ත/න) get proportionally less attention.
TRIVIAL_KEEP_RATE = 0.25


def _is_trivial(ocr: str, gold: str) -> bool:
    """True when the only difference is invisible characters or spacing."""
    squash = lambda s: " ".join(s.replace(ZWNJ, "").split())   # noqa: E731
    return squash(ocr) == squash(gold)


def _read_pair(r):
    """
    Pull (wrong text, right text) out of a row under either naming scheme.

    The retired corpora used `ocr`/`gold`. Pipeline B writes `raw`/`corrected`
    because on production input there IS no gold -- the corrected column is
    model output, and calling it `gold` in the file would be a lie that
    outlives this project. Accept both rather than rewriting either.
    """
    if "ocr" in r or "gold" in r:
        return r.get("ocr", "").strip(), r.get("gold", "").strip()
    return r.get("raw", "").strip(), r.get("corrected", "").strip()


def load_pairs(paths):
    """Read the JSONL files and keep only pairs worth learning from."""
    rows, skipped, trimmed, oversize = [], 0, 0, 0
    for p in paths:
        if not os.path.exists(p):
            print(f"  missing, skipping: {p}")
            continue
        with open(p, encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                ocr, gold = _read_pair(r)

                # Drop pairs that teach nothing or teach the wrong thing:
                #   empty            - no signal
                #   identical        - the model would learn to copy, and
                #                      16% of gazette lines are already perfect
                #   wildly different - OCR failed so badly the correct text
                #                      cannot be inferred; training on these
                #                      teaches the model to invent Sinhala
                if not ocr or not gold:
                    skipped += 1
                elif (len(ocr.encode("utf-8")) > MAX_LENGTH
                      or len(gold.encode("utf-8")) > MAX_LENGTH):
                    # Too long for the window. Counted and reported rather
                    # than truncated -- see the header. A page pair lands
                    # here every time, which is the signal that the input
                    # was never split into lines.
                    oversize += 1
                elif ocr == gold:
                    skipped += 1
                elif cer(gold, ocr) > 0.6:
                    skipped += 1
                elif _is_trivial(ocr, gold) and random.random() > TRIVIAL_KEEP_RATE:
                    trimmed += 1
                else:
                    rows.append({"input": ocr, "target": gold})

    print(f"usable pairs: {len(rows):,}")
    print(f"  skipped (empty / identical / unrecoverable): {skipped:,}")
    print(f"  trimmed (trivial invisible-character pairs): {trimmed:,}")
    print(f"  oversize (longer than {MAX_LENGTH} bytes): {oversize:,}")

    # Stop rather than train on the leftovers. If most of the input is too
    # long, it is page-level and the run would silently be measuring a
    # handful of unusually short pages instead of the corpus.
    seen = len(rows) + skipped + trimmed + oversize
    if seen and oversize > 0.3 * seen:
        sys.exit(
            f"\n{100 * oversize / seen:.0f}% of pairs exceed {MAX_LENGTH} bytes.\n"
            "That is what page-level input looks like: an acts-1010 page is\n"
            "~4,800 bytes in Sinhala UTF-8 and this model reads 384 at a time.\n"
            "Split the pages into aligned lines first, then run this again."
        )
    return rows


pairs = load_pairs(TRAIN_FILES)
random.shuffle(pairs)

split = int(len(pairs) * 0.95)
train_ds = Dataset.from_list(pairs[:split])
val_ds = Dataset.from_list(pairs[split:])
print(f"train {len(train_ds):,} | validation {len(val_ds):,}")


# ══════════════════════════════════════════════════════════════
#  4. Tokenize
# ══════════════════════════════════════════════════════════════

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME)


def preprocess(batch):
    enc = tokenizer(
        batch["input"], max_length=MAX_LENGTH, truncation=True, padding=False
    )
    labels = tokenizer(
        batch["target"], max_length=MAX_LENGTH, truncation=True, padding=False
    )
    enc["labels"] = labels["input_ids"]
    return enc


train_tok = train_ds.map(preprocess, batched=True, remove_columns=train_ds.column_names)
val_tok = val_ds.map(preprocess, batched=True, remove_columns=val_ds.column_names)


# ══════════════════════════════════════════════════════════════
#  5. Scoring during training
# ══════════════════════════════════════════════════════════════

def compute_metrics(eval_pred):
    """
    Report CER and WER rather than loss. Loss falling is not the same as
    the text getting better, and CER is the number the thesis reports.
    """
    preds, labels = eval_pred
    if isinstance(preds, tuple):
        preds = preds[0]

    preds = np.where(preds != -100, preds, tokenizer.pad_token_id)
    labels = np.where(labels != -100, labels, tokenizer.pad_token_id)

    pred_txt = tokenizer.batch_decode(preds, skip_special_tokens=True)
    gold_txt = tokenizer.batch_decode(labels, skip_special_tokens=True)

    pairs_ = [(g, p) for g, p in zip(gold_txt, pred_txt) if g.strip()]
    if not pairs_:
        return {"cer": 1.0, "wer": 1.0}

    return {
        "cer": float(np.mean([cer(g, p) for g, p in pairs_])),
        "wer": float(np.mean([wer(g, p) for g, p in pairs_])),
    }


# ══════════════════════════════════════════════════════════════
#  6. Train
# ══════════════════════════════════════════════════════════════

# Schedule sized from the actual corpus, not from a fixed guess.
#
# WHY THIS IS NOT `warmup_steps=500` ANY MORE. The run killed on 2026-08-15
# had 1,214 total steps, so 500 warmup meant 41% of training was spent
# ramping the learning rate up, and it decayed to zero the moment it peaked.
# The convention is 5-10%. At step 500 that run scored CER 0.1451 against a
# do-nothing baseline of 0.0940 -- it was actively making text worse.
#
# The fixed 500 for eval_steps/save_steps was the same trap from the other
# side. This corpus is far smaller than the retired one: ~90 pages of line
# pairs is a few hundred steps in total, so a 500-step evaluation interval
# would never fire even once, `metric_for_best_model="cer"` would have
# nothing to compare, and load_best_model_at_end would fail at the end of a
# completed run. Both intervals are now derived from the real step count.
_STEPS_PER_EPOCH = math.ceil(len(train_ds) / (BATCH_SIZE * GRAD_ACCUM))
_TOTAL_STEPS = max(1, _STEPS_PER_EPOCH * EPOCHS)
WARMUP_STEPS = max(10, round(_TOTAL_STEPS * 0.06))
CHECK_EVERY = max(10, _TOTAL_STEPS // 8)

print(f"\nschedule: {_TOTAL_STEPS} steps over {EPOCHS} epoch(s) "
      f"({_STEPS_PER_EPOCH}/epoch), warmup {WARMUP_STEPS} "
      f"({100 * WARMUP_STEPS / _TOTAL_STEPS:.0f}%), evaluating every {CHECK_EVERY}")

args = Seq2SeqTrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE,
    gradient_accumulation_steps=GRAD_ACCUM,
    learning_rate=LEARNING_RATE,
    num_train_epochs=EPOCHS,
    warmup_steps=WARMUP_STEPS,

    # Recompute intermediate values during the backward pass instead of
    # holding all of them. Roughly 30% slower per step, but it is the
    # single largest saving available and it is what makes this fit.
    gradient_checkpointing=True,

    eval_strategy="steps",
    eval_steps=CHECK_EVERY,
    save_strategy="steps",
    save_steps=CHECK_EVERY,
    save_total_limit=2,

    # Keep whichever checkpoint scored best, not whichever came last —
    # training loss keeps falling after real quality starts degrading.
    load_best_model_at_end=True,
    metric_for_best_model="cer",
    greater_is_better=False,

    predict_with_generate=True,
    generation_max_length=MAX_LENGTH,
    # FP16 IS OFF DELIBERATELY — DO NOT TURN IT BACK ON.
    #
    # The T4 chip supports half precision, but T5-family models do not
    # survive it. They were pretrained in bfloat16, whose exponent range is
    # far wider than fp16's; T5's internal activations routinely exceed what
    # fp16 can represent, overflow to inf, and the loss becomes nan.
    #
    # Observed here directly: training loss 14,344,448 and validation loss
    # nan by step 500, giving CER 1.18 — worse than the uncorrected OCR.
    #
    # bf16 would be the correct answer, but the T4 is a Turing card and has
    # no bf16 support. That leaves fp32: roughly half the speed, and stable.
    fp16=False,
    bf16=False,

    # Print the loss often. If it is not in single digits within the first
    # few hundred steps, the run has diverged and is worth killing early
    # rather than four hours later.
    logging_steps=25,
    report_to="none",
    seed=SEED,
)

trainer = Seq2SeqTrainer(
    model=model,
    args=args,
    train_dataset=train_tok,
    eval_dataset=val_tok,
    data_collator=DataCollatorForSeq2Seq(tokenizer, model=model),
    compute_metrics=compute_metrics,
)

trainer.train()

trainer.save_model(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)
print(f"\nSaved to {OUTPUT_DIR}/ — download this folder.")


# ══════════════════════════════════════════════════════════════
#  7. Did it actually help?
# ══════════════════════════════════════════════════════════════
#
# This must be measured on the held-out test set, not on the validation
# split. They are not the same yardstick and cannot be compared:
#
#   validation split  5% carved out of the pipeline's own generated pairs,
#                     AFTER load_pairs() removed the already-correct ones
#                     and thinned the easy invisible-character ones. What is
#                     left is deliberately the hard remainder. Its corrected
#                     side is MODEL OUTPUT, not human-verified truth.
#   test set          the 202 human-transcribed acts-1010 pages, nothing
#                     filtered, carrying Tesseract's CER for each page in
#                     data/acts1010/_tesseract_test_split.jsonl.
#
# The 0.1079 baseline was measured on the second, and the second is the only
# gold text in this project. Scoring the model on the first and comparing
# the two numbers is how a model that is helping gets thrown away, or one
# that is hurting gets shipped.

model.eval()
device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)

EVAL_BATCH = 16


def correct_batch(texts):
    """Run the model over a list of lines and return its output."""
    enc = tokenizer(
        texts, return_tensors="pt", padding=True,
        max_length=MAX_LENGTH, truncation=True,
    ).to(device)
    with torch.no_grad():
        out = model.generate(**enc, max_length=MAX_LENGTH)
    return tokenizer.batch_decode(out, skip_special_tokens=True)


def evaluate_on_test_set(paths):
    """
    Score the model against the real scans, row for row, against the same
    rows' Tesseract CER. Returns None when no test set is attached.
    """
    # Keep every row that has gold text — including the 23 where Tesseract
    # returned nothing at all. Those score CER 1.0 and are part of the
    # recorded baseline; dropping them would quietly move the goalposts and
    # flatter the model by comparison.
    rows = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                if r.get("gold", "").strip():
                    rows.append(r)
    if not rows:
        return None

    print(f"\nscoring {len(rows):,} held-out pages (this takes a few minutes) ...")

    # CORRECT LINE BY LINE, THEN REASSEMBLE THE PAGE.
    #
    # A test row is a whole page -- roughly twelve times this model's byte
    # window. Feeding it in whole would truncate it, so the model would be
    # scored on the first 8% of each page against the whole of the gold
    # text: a number that looks real and means nothing.
    #
    # Splitting, correcting and rejoining keeps the final CER page-level,
    # which is what makes it comparable to Tesseract's 0.1079 on these same
    # pages. Blank lines pass through untouched so the page's line structure
    # survives reassembly.
    flat, owner = [], []
    for n, r in enumerate(rows):
        for ln in r["ocr"].split("\n"):
            flat.append(ln)
            owner.append(n)

    out = list(flat)
    todo = [i for i, ln in enumerate(flat) if ln.strip()]
    for i in range(0, len(todo), EVAL_BATCH):
        idxs = todo[i:i + EVAL_BATCH]
        for j, fixed in zip(idxs, correct_batch([flat[j] for j in idxs])):
            out[j] = fixed
        if i % (EVAL_BATCH * 50) == 0:
            print(f"  {i:,}/{len(todo):,} lines")

    rebuilt = [[] for _ in rows]
    for line_out, n in zip(out, owner):
        rebuilt[n].append(line_out)
    preds = ["\n".join(parts) for parts in rebuilt]

    cer_raw = [cer(r["gold"], r["ocr"]) for r in rows]
    cer_model = [cer(r["gold"], p) for r, p in zip(rows, preds)]

    # How often correction makes a line worse, not better. This number
    # belongs in the thesis regardless of which way the average goes.
    worse = sum(m > b for m, b in zip(cer_model, cer_raw))

    return {
        "n": len(rows),
        "tesseract": float(np.mean(cer_raw)),
        "model": float(np.mean(cer_model)),
        "model_wer": float(np.mean([wer(r["gold"], p) for r, p in zip(rows, preds)])),
        "worse": worse,
    }


final = trainer.evaluate()
print(f"\nvalidation split (training corpus, filtered): "
      f"CER {final.get('eval_cer', float('nan')):.4f}  "
      f"WER {final.get('eval_wer', float('nan')):.4f}")
print("  ^ a progress indicator only — NOT comparable to the baseline below.")

result = evaluate_on_test_set(EVAL_FILES)

if result is None:
    print("\nNo test set attached, so the question 'did it help' is unanswered.")
    print("Attach the held-out test pairs to the Kaggle dataset and re-run")
    print("this section, or score the downloaded model locally.")
else:
    # PAGES, not lines. The unit changed on 2026-08-27 and the labels did
    # not follow: the model corrects line by line, but each row scored here
    # is a whole page reassembled from those lines, compared against the
    # gold page. That is what makes the figure comparable to Tesseract's
    # 0.1079, which is also a per-page mean. Reporting it as a line-level
    # score would be a different measurement with a different denominator.
    print(f"\n── HELD-OUT TEST SET ({result['n']:,} held-out pages) ──")
    print(f"Tesseract alone : {result['tesseract']:.4f} CER")
    print(f"With the model  : {result['model']:.4f} CER   (WER {result['model_wer']:.4f})")
    print(f"Pages made worse by correction: {result['worse']:,} "
          f"({100 * result['worse'] / result['n']:.1f}%)")

    # Cross-check against the value recorded when the test set was built. A
    # mismatch means the test set changed underneath this constant.
    if abs(result["tesseract"] - TESSERACT_BASELINE) > 0.005:
        print(f"  note: measured baseline differs from the recorded "
              f"{TESSERACT_BASELINE:.4f} — the test set has changed.")

    if result["model"] < result["tesseract"]:
        drop = 100 * (result["tesseract"] - result["model"]) / result["tesseract"]
        print(f"-> {drop:.1f}% fewer character errors. It works.")
    else:
        print("-> The model is making things worse. Do not deploy it.")
        print("   Usual causes: too little data, too few epochs, or a")
        print("   learning rate that needs lowering.")


# ══════════════════════════════════════════════════════════════
#  8. Look at real output before trusting the number
# ══════════════════════════════════════════════════════════════

for ex in val_ds.select(range(min(5, len(val_ds)))):
    ids = tokenizer(ex["input"], return_tensors="pt",
                    max_length=MAX_LENGTH, truncation=True).to(device)
    with torch.no_grad():
        out = model.generate(**ids, max_length=MAX_LENGTH)
    print("\nOCR   :", ex["input"])
    print("MODEL :", tokenizer.decode(out[0], skip_special_tokens=True))
    print("GOLD  :", ex["target"])
