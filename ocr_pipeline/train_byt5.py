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
Tesseract alone scores 0.0921 CER on the evaluation set. Anything above
that means the model is making the text worse.
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
        found = sorted(glob.glob(os.path.join("data", "**", "*.jsonl"), recursive=True))

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
EPOCHS = 2              # fp32 costs ~2x the time per step, see FP16 below
SEED = 42

TESSERACT_BASELINE = 0.0921   # CER to beat, measured by build_test_set.py

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


def load_pairs(paths):
    """Read the JSONL files and keep only pairs worth learning from."""
    rows, skipped, trimmed = [], 0, 0
    for p in paths:
        if not os.path.exists(p):
            print(f"  missing, skipping: {p}")
            continue
        with open(p, encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                ocr, gold = r.get("ocr", "").strip(), r.get("gold", "").strip()

                # Drop pairs that teach nothing or teach the wrong thing:
                #   empty            - no signal
                #   identical        - the model would learn to copy, and
                #                      16% of gazette lines are already perfect
                #   wildly different - OCR failed so badly the correct text
                #                      cannot be inferred; training on these
                #                      teaches the model to invent Sinhala
                if not ocr or not gold:
                    skipped += 1
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

args = Seq2SeqTrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE,
    gradient_accumulation_steps=GRAD_ACCUM,
    learning_rate=LEARNING_RATE,
    num_train_epochs=EPOCHS,
    warmup_steps=500,

    # Recompute intermediate values during the backward pass instead of
    # holding all of them. Roughly 30% slower per step, but it is the
    # single largest saving available and it is what makes this fit.
    gradient_checkpointing=True,

    eval_strategy="steps",
    eval_steps=500,
    save_strategy="steps",
    save_steps=500,
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
#   validation split  1,022 lines carved out of the gazette + Ransaka
#                     corpus, AFTER load_pairs() removed the already-correct
#                     pairs and thinned the easy invisible-character ones.
#                     What is left is deliberately the hard remainder.
#   test set          1,078 real scanned pages, human-transcribed, nothing
#                     filtered, and carrying the Tesseract CER for each row.
#
# The 0.0921 baseline was measured on the second. Scoring the model on the
# first and comparing the two numbers is how a model that is helping gets
# thrown away, or a model that is hurting gets shipped.

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
    # recorded 0.0921 baseline; dropping them would quietly move the
    # goalposts to 0.0723 and flatter the model by comparison.
    rows = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                if r.get("gold", "").strip():
                    rows.append(r)
    if not rows:
        return None

    print(f"\nscoring {len(rows):,} held-out lines (this takes a few minutes) ...")

    preds = []
    for i in range(0, len(rows), EVAL_BATCH):
        chunk = rows[i:i + EVAL_BATCH]
        preds.extend(correct_batch([r["ocr"] for r in chunk]))
        if i % (EVAL_BATCH * 20) == 0:
            print(f"  {i:,}/{len(rows):,}")

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
    print("Attach data/eval/test_set.jsonl to the Kaggle dataset and re-run")
    print("this section, or score the downloaded model locally.")
else:
    print(f"\n── HELD-OUT TEST SET ({result['n']:,} real scanned lines) ──")
    print(f"Tesseract alone : {result['tesseract']:.4f} CER")
    print(f"With the model  : {result['model']:.4f} CER   (WER {result['model_wer']:.4f})")
    print(f"Lines made worse by correction: {result['worse']:,} "
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
