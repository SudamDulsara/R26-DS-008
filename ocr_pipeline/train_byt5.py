"""
Fine-tune ByT5 for Sinhala post-OCR correction
===============================================
Runs on Kaggle's free T4 GPU. Each numbered section below is one notebook
cell — paste them in order, or run the file as a script.

    Kaggle → New Notebook → Settings → Accelerator: GPU T4 x2
    (phone verification is required before Kaggle offers a GPU)

INPUT  : gazette_pairs.jsonl + ocr_pairs.jsonl  (upload as a Kaggle Dataset)
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
# !pip install -q transformers datasets accelerate jiwer sentencepiece

import json
import os
import random

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

# On Kaggle these become /kaggle/input/<your-dataset-name>/...
TRAIN_FILES = [
    "data/training/gazette_pairs.jsonl",
    "data/training/ocr_pairs.jsonl",
]
OUTPUT_DIR = "byt5-sinhala-ocr"

# Sinhala is 3 bytes per character in UTF-8, so a byte-level model sees
# roughly 3x the length it would for English. Lines here average ~42
# characters (~126 bytes); 512 leaves generous headroom for the longest.
# Raising this is the fastest way to run out of GPU memory.
MAX_LENGTH = 512

BATCH_SIZE = 8          # what fits on a 16 GB T4 at this length
GRAD_ACCUM = 4          # effective batch of 32
LEARNING_RATE = 3e-4    # standard range for fine-tuning T5-family models
EPOCHS = 3
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
    fp16=torch.cuda.is_available(),   # T4 supports fp16, not bf16
    logging_steps=100,
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

final = trainer.evaluate()
model_cer = final.get("eval_cer", 1.0)

print(f"\nTesseract alone : {TESSERACT_BASELINE:.4f} CER")
print(f"With the model  : {model_cer:.4f} CER")

if model_cer < TESSERACT_BASELINE:
    drop = 100 * (TESSERACT_BASELINE - model_cer) / TESSERACT_BASELINE
    print(f"-> {drop:.1f}% fewer character errors. It works.")
else:
    print("-> The model is making things worse. Do not deploy it.")
    print("   Usual causes: too little data, too few epochs, or a")
    print("   learning rate that needs lowering.")


# ══════════════════════════════════════════════════════════════
#  8. Look at real output before trusting the number
# ══════════════════════════════════════════════════════════════

model.eval()
device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)

for ex in val_ds.select(range(min(5, len(val_ds)))):
    ids = tokenizer(ex["input"], return_tensors="pt",
                    max_length=MAX_LENGTH, truncation=True).to(device)
    with torch.no_grad():
        out = model.generate(**ids, max_length=MAX_LENGTH)
    print("\nOCR   :", ex["input"])
    print("MODEL :", tokenizer.decode(out[0], skip_special_tokens=True))
    print("GOLD  :", ex["target"])
