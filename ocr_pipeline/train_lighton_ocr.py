"""
Fine-tune LightOnOCR-2-1B for Sinhala page-level OCR
====================================================
Runs on Kaggle's free T4. Paste-and-run: it finds its own data and needs no
path edits.

    Kaggle -> New Notebook -> Settings -> Accelerator: GPU T4 x2
    Add Input -> HuggingFace dataset avishadilhara/sinhala-ocr-lk-acts-1010
    (or let this script pull it from the Hub, which is the default)

INPUT  : acts-1010 page images + human transcriptions
OUTPUT : a LoRA adapter you download and load in Pipeline B


WHAT THIS IS FOR
----------------
This is Pipeline A — scaffolding. It produces the model that supplies the
CORRECTED side of every pair Pipeline B publishes. Tesseract supplies the raw
side. Neither model is the research contribution; the dataset is.

THE NUMBER TO BEAT
------------------
Tesseract scores 0.1079 CER on the 202 held-out test pages, measured in this
project on 2026-08-16. The paper that published this dataset independently
reports 0.1069 for Tesseract v5 and reached 0.0105 by fine-tuning this same
model with QLoRA. So ~0.01 is the target and ~0.11 is the floor.

If this lands near 0.05 rather than 0.01, the corrected column is not clean
enough to be worth publishing and Route B needs reconsidering. That is the
decision this script exists to inform.


WHY NOT JUST USE THE MODEL AS-IS
--------------------------------
LightOnOCR-2-1B lists 11 languages and Sinhala is not among them. Untuned, it
has no particular reason to read Sinhala well. The 1.05% in the paper is after
their fine-tune, not before it. That is also why using it is not "correcting
text with someone else's model" — the Sinhala capability is the part this
project adds.


READ THIS BEFORE THE FIRST RUN
------------------------------
Unlike train_byt5.py, this script has never been executed. It is written from
the transformers documentation for LightOnOcrForConditionalGeneration. The
first run should be a smoke test, not a four-hour job:

    SMOKE=1     -> 12 training pages, 6 eval pages, 2 test pages, ~5 minutes

That exercises every line of the script cheaply. Only once it completes should
you run it for real. Set SMOKE=0 (or unset it) for the full run.
"""

# ══════════════════════════════════════════════════════════════
#  1. Install
# ══════════════════════════════════════════════════════════════
# LightOnOCR is a recent addition to transformers. Kaggle's preinstalled
# version may predate it entirely, in which case importing the model class
# raises ImportError. Upgrading transformers on Kaggle can force a kernel
# restart -- if that happens, just run the cell again; the second run finds
# everything already installed and continues.
#
# peft and bitsandbytes are what make QLoRA possible and are not preinstalled.

import importlib.util
import subprocess
import sys


def _pip(*args):
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", *args], check=True)


def _ensure(pkg, spec=None):
    if importlib.util.find_spec(pkg) is None:
        print(f"installing {pkg} ...")
        _pip(spec or pkg)


_ensure("jiwer")
_ensure("peft")
_ensure("bitsandbytes")

# Check the model class exists before doing anything expensive.
try:
    from transformers import LightOnOcrForConditionalGeneration  # noqa: F401
    print("transformers already knows LightOnOcr")
except ImportError:
    print("upgrading transformers for LightOnOcr support ...")
    _pip("-U", "transformers")
    print("\n*** If the kernel restarts now, simply run this cell again. ***\n")
    from transformers import LightOnOcrForConditionalGeneration  # noqa: F401

import json
import os
import random
import unicodedata

# ── Must be set before torch is imported ────────────────────────────
#
# One GPU, not two. Kaggle offers T4 x2 and the Trainer would wrap the model
# in DataParallel, replicating it on both cards and gathering every output
# onto card 0 -- which is the card that then runs out. This exact failure cost
# a run on this project already.
os.environ["CUDA_VISIBLE_DEVICES"] = "0"

# Lets the allocator grow segments rather than fragment them.
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import numpy as np
import torch
from datasets import load_dataset
from jiwer import cer
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
from PIL import Image
from transformers import (
    AutoProcessor,
    BitsAndBytesConfig,
    Trainer,
    TrainingArguments,
)


# ══════════════════════════════════════════════════════════════
#  2. Settings
# ══════════════════════════════════════════════════════════════

MODEL_NAME = "lightonai/LightOnOCR-2-1B"
DATASET_NAME = "avishadilhara/sinhala-ocr-lk-acts-1010"
OUTPUT_DIR = "lightonocr-sinhala-acts"

SMOKE = os.environ.get("SMOKE", "0") == "1"

#: Tesseract's score on the 202 test pages, measured in this project.
#: Do NOT substitute the paper's 0.1069 -- quote the number you measured
#: under the same conditions as everything else you report.
TESSERACT_BASELINE = 0.1079

#: Longest page transcription seen in the 202 test pages is ~4,300 bytes;
#: pages average ~39 lines of ~105 bytes. 2048 tokens clears that with room,
#: and attention cost grows with the square of this number.
MAX_TARGET_TOKENS = 2048

#: Cap on the longer image edge, in pixels.
#:
#: This is the single biggest memory lever and the one most likely to need
#: tuning. A Pixtral-style vision encoder turns a page into visual tokens in
#: proportion to its area, so a full-resolution A4 scan can produce thousands
#: of them and exhaust a 16 GB T4 before training even starts.
#:
#: It is also a genuine trade-off against accuracy: OCR needs resolution, and
#: downscaling too far will destroy the very diacritics this project cares
#: about. 1536 keeps roughly 150 DPI on an A4 page, which is above the usual
#: floor for reliable text recognition.
#:
#: If you hit out-of-memory, lower this BEFORE touching batch size -- there is
#: no batch size below 1, and image area is where the memory actually goes.
MAX_IMAGE_EDGE = 1536

BATCH_SIZE = 1          # one page per step; pages are large
GRAD_ACCUM = 8          # effective batch of 8
LEARNING_RATE = 1e-4    # standard for LoRA; the base weights stay frozen
EPOCHS = 3
SEED = 42

#: 5% of the run, not 41% of it.
#:
#: The ByT5 run killed on 2026-08-15 used warmup_steps=500 against 1,214 total
#: steps, so the learning rate spent most of the run climbing and then decayed
#: to zero almost immediately after peaking. CER at step 500 was 0.1451
#: against a do-nothing baseline of 0.0940 -- the model was making text worse.
#: With 707 pages at an effective batch of 8, this run has only ~265 steps in
#: total, so a fixed warmup_steps would be even more disproportionate.
WARMUP_RATIO = 0.05

if SMOKE:
    EPOCHS = 1
    print("*** SMOKE TEST -- tiny subsets, results are meaningless ***")

random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

if not torch.cuda.is_available():
    sys.exit("No GPU. Kaggle: Settings -> Accelerator -> GPU T4 x2.")
print("GPU:", torch.cuda.get_device_name(0))
print(f"VRAM: {torch.cuda.get_device_properties(0).total_memory / 1e9:.1f} GB")


# ══════════════════════════════════════════════════════════════
#  3. Normalisation
# ══════════════════════════════════════════════════════════════
# A faithful copy of pipeline/normalize.py, inlined because Kaggle has no
# access to this repo. It MUST stay in step with that file: every CER number
# in this project is measured on normalised text, and comparing a differently
# normalised number to 0.1079 would be meaningless.
#
# The reason it exists: the same Sinhala word can be encoded two ways
# (composed vs decomposed vowel signs). Measured through raw jiwer, two valid
# encodings of identical text scored CER 0.5 -- 50% error on correct text.
#
# ZWNJ and ZWJ are deliberately PRESERVED. They change what the text says.

_NOISE_INVISIBLES = frozenset({
    "﻿",  # BOM / zero-width no-break space
    "​",  # zero-width space
    "‎",  # left-to-right mark
    "‏",  # right-to-left mark
    "⁠",  # word joiner
    "­",  # soft hyphen
})

_SPACE_VARIANTS = frozenset({
    " ", " ", " ", " ", " ", " ", " ",
    " ", " ", " ", " ", " ", " ", " ",
    "　", "\t",
})


def normalize(text: str) -> str:
    """Canonicalise encoding. Never corrects errors."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    out = []
    for ch in text:
        if ch in _NOISE_INVISIBLES:
            continue
        out.append(" " if ch in _SPACE_VARIANTS else ch)
    text = unicodedata.normalize("NFC", "".join(out))
    text = "\n".join(line.rstrip(" ") for line in text.split("\n"))
    return text.strip("\n ")


# ══════════════════════════════════════════════════════════════
#  4. Load the data
# ══════════════════════════════════════════════════════════════
# The test split is never loaded for training and never generated from during
# training. It is the only human-verified truth in the project and the sole
# source of every number the thesis reports.


def find_local_dataset():
    """Prefer an attached Kaggle copy over re-downloading 872 MB."""
    for root, dirs, _files in os.walk("/kaggle/input"):
        if "data" in dirs and any(
            f.endswith(".parquet") for f in os.listdir(os.path.join(root, "data"))
        ):
            return root
    return None


_local = find_local_dataset()
_source = _local or DATASET_NAME
print(f"dataset source: {_source}")

train_ds = load_dataset(_source, split="train")
eval_ds = load_dataset(_source, split="eval")
test_ds = load_dataset(_source, split="test")

if SMOKE:
    train_ds = train_ds.select(range(12))
    eval_ds = eval_ds.select(range(6))
    test_ds = test_ds.select(range(2))

print(f"train {len(train_ds)} | eval {len(eval_ds)} | test {len(test_ds)} (held out)")


def shrink(image: Image.Image) -> Image.Image:
    """Cap the longer edge, preserving aspect ratio. See MAX_IMAGE_EDGE."""
    if image.mode != "RGB":
        image = image.convert("RGB")
    w, h = image.size
    longest = max(w, h)
    if longest <= MAX_IMAGE_EDGE:
        return image
    scale = MAX_IMAGE_EDGE / longest
    return image.resize((int(w * scale), int(h * scale)), Image.LANCZOS)


_sample = train_ds[0]["image"]
print(f"original page size: {_sample.size} -> after shrink: {shrink(_sample).size}")


# ══════════════════════════════════════════════════════════════
#  5. Model + QLoRA
# ══════════════════════════════════════════════════════════════
# 4-bit quantisation puts a 1B vision-language model on a 16 GB card with room
# for activations. The base weights stay frozen and quantised; only small
# adapter matrices train, which is what makes this fit at all.
#
# compute dtype is float16, not bfloat16: the T4 is a Turing card and has no
# bf16 support. Note this is NOT the fp16 trap that destroyed the ByT5 run --
# that was specific to T5-family models, which are pretrained in bfloat16 and
# whose activations overflow fp16's exponent range. This model's decoder is
# Qwen3-based and trains in fp16 routinely.
#
# Watch the loss anyway. If it goes to nan in the first hundred steps, fp16 is
# the first suspect and the only fix on a T4 is fp32, at roughly half speed.

processor = AutoProcessor.from_pretrained(MODEL_NAME)

quant_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_use_double_quant=True,
    bnb_4bit_compute_dtype=torch.float16,
)

model = LightOnOcrForConditionalGeneration.from_pretrained(
    MODEL_NAME,
    quantization_config=quant_config,
    dtype=torch.float16,
    device_map={"": 0},
)

model = prepare_model_for_kbit_training(model, use_gradient_checkpointing=True)
model.config.use_cache = False       # incompatible with gradient checkpointing

# Adapt the text decoder, not the vision encoder. The encoder already reads
# printed glyphs well -- it was trained on documents. What it has never had to
# do is emit Sinhala, and that is the decoder's job. Adapting only the decoder
# also roughly halves the trainable parameters.
lora_config = LoraConfig(
    r=16,
    lora_alpha=32,
    lora_dropout=0.05,
    bias="none",
    task_type="CAUSAL_LM",
    target_modules=["q_proj", "k_proj", "v_proj", "o_proj",
                    "gate_proj", "up_proj", "down_proj"],
)

model = get_peft_model(model, lora_config)
model.print_trainable_parameters()


# ══════════════════════════════════════════════════════════════
#  6. Turning a page into a training example
# ══════════════════════════════════════════════════════════════
# The model is a chat model: it sees a user turn holding an image and replies
# with the page's text. So each training example is that whole exchange, with
# the loss computed ONLY over the reply.
#
# Masking the prompt matters. Without it the model spends capacity learning to
# predict its own prompt template -- tokens that are identical in every single
# example and teach nothing about reading Sinhala.

PROMPT_MESSAGES = [{"role": "user", "content": [{"type": "image"}]}]


def build_texts(gold: str):
    """Return (prompt_only_text, full_conversation_text)."""
    prompt = processor.apply_chat_template(
        PROMPT_MESSAGES, add_generation_prompt=True, tokenize=False
    )
    full = processor.apply_chat_template(
        PROMPT_MESSAGES + [{
            "role": "assistant",
            "content": [{"type": "text", "text": gold}],
        }],
        tokenize=False,
    )
    return prompt, full


def collate(batch):
    """
    Build a padded batch, with labels masked over the prompt.

    Done one example at a time because the prompt length depends on the image
    (each image expands into a variable number of visual tokens), so the mask
    boundary differs per row.
    """
    images, fulls, prompt_lens = [], [], []

    for ex in batch:
        image = shrink(ex["image"])
        gold = normalize(ex["text"])
        prompt_text, full_text = build_texts(gold)

        # Length of the prompt *for this image*, so we know where the
        # model's own reply begins.
        p = processor(images=[image], text=[prompt_text], return_tensors="pt")
        prompt_lens.append(p["input_ids"].shape[1])

        images.append(image)
        fulls.append(full_text)

    enc = processor(
        images=images,
        text=fulls,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=MAX_TARGET_TOKENS,
    )

    labels = enc["input_ids"].clone()

    pad_id = processor.tokenizer.pad_token_id
    if pad_id is not None:
        labels[labels == pad_id] = -100

    # Never train on the image placeholder tokens either -- they are inputs,
    # not something the model should learn to produce.
    image_token_id = getattr(model.config, "image_token_id", None)
    if image_token_id is not None:
        labels[enc["input_ids"] == image_token_id] = -100

    for i, plen in enumerate(prompt_lens):
        labels[i, :plen] = -100

    enc["labels"] = labels
    return enc


# Sanity-check one example before committing to a training run. If the target
# text is empty after masking, every gradient would be zero and the run would
# silently learn nothing.
_probe = collate([train_ds[0]])
_kept = (_probe["labels"] != -100).sum().item()
print(f"probe example: {_probe['input_ids'].shape[1]} tokens, "
      f"{_kept} of them supervised")
if _kept == 0:
    sys.exit("Prompt masking removed every label. Fix build_texts() before training.")


# ══════════════════════════════════════════════════════════════
#  7. Train
# ══════════════════════════════════════════════════════════════

args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE,
    gradient_accumulation_steps=GRAD_ACCUM,
    learning_rate=LEARNING_RATE,
    num_train_epochs=EPOCHS,
    warmup_ratio=WARMUP_RATIO,

    gradient_checkpointing=True,
    gradient_checkpointing_kwargs={"use_reentrant": False},

    eval_strategy="epoch",
    save_strategy="epoch",
    save_total_limit=2,
    load_best_model_at_end=True,
    metric_for_best_model="eval_loss",
    greater_is_better=False,

    fp16=True,
    bf16=False,          # T4 is Turing; no bf16 support exists on this card

    logging_steps=5,     # ~265 steps total, so log often
    report_to="none",
    seed=SEED,
    remove_unused_columns=False,   # the collator needs `image` and `text`
    dataloader_num_workers=2,
)

trainer = Trainer(
    model=model,
    args=args,
    train_dataset=train_ds,
    eval_dataset=eval_ds,
    data_collator=collate,
)

trainer.train()

trainer.save_model(OUTPUT_DIR)
processor.save_pretrained(OUTPUT_DIR)
print(f"\nAdapter saved to {OUTPUT_DIR}/ -- download this folder.")


# ══════════════════════════════════════════════════════════════
#  8. Did it beat Tesseract?
# ══════════════════════════════════════════════════════════════
# Measured on the 202 held-out test pages -- the same pages, the same
# normalisation and the same per-page-mean CER that produced 0.1079. Nothing
# else is comparable to that number.
#
# This is slow: a full page is a long generation and there are 202 of them.
# Budget 30-60 minutes. EVAL_LIMIT exists for a quick partial read.

EVAL_LIMIT = int(os.environ.get("EVAL_LIMIT", "0"))

model.eval()
model.config.use_cache = True


@torch.no_grad()
def read_page(image):
    """Run the fine-tuned model over one page and return the text."""
    prompt_text = processor.apply_chat_template(
        PROMPT_MESSAGES, add_generation_prompt=True, tokenize=False
    )
    inputs = processor(
        images=[shrink(image)], text=[prompt_text], return_tensors="pt"
    ).to(model.device)

    out = model.generate(
        **inputs,
        max_new_tokens=MAX_TARGET_TOKENS,
        do_sample=False,            # greedy: reading a page is not creative
    )
    reply = out[0, inputs["input_ids"].shape[1]:]
    return processor.decode(reply, skip_special_tokens=True)


rows = range(len(test_ds)) if not EVAL_LIMIT else range(min(EVAL_LIMIT, len(test_ds)))
results = []

print(f"\nreading {len(list(rows))} held-out pages ...")
for n, i in enumerate(rows):
    ex = test_ds[i]
    gold = normalize(ex["text"])
    pred = normalize(read_page(ex["image"]))
    if not gold.strip():
        continue
    results.append({
        "idx": i,
        "year": ex.get("year"),
        "cer_model": cer(gold, pred),
        "pred": pred,
    })
    if (n + 1) % 10 == 0:
        running = np.mean([r["cer_model"] for r in results])
        print(f"  {n + 1}/{len(list(rows))}  running mean CER {running:.4f}")

model_cer = float(np.mean([r["cer_model"] for r in results]))

print(f"\n== HELD-OUT TEST SET ({len(results)} pages) ==")
print(f"Tesseract        : {TESSERACT_BASELINE:.4f} CER   (measured 2026-08-16)")
print(f"This fine-tune   : {model_cer:.4f} CER")
print(f"Paper's target   : 0.0105 CER   (same model, same data, their QLoRA run)")

if model_cer < TESSERACT_BASELINE:
    drop = 100 * (TESSERACT_BASELINE - model_cer) / TESSERACT_BASELINE
    print(f"-> {drop:.1f}% fewer character errors than Tesseract.")
else:
    print("-> WORSE than Tesseract. Do not use this as the corrected column.")

# The era split is where the project's argument lives: Tesseract scores 0.1650
# on 1980s scans against 0.0688 on modern print. Whether the fine-tune closes
# that gap is a finding in its own right.
by_era = {"1981-1989": [], "2000-2019": []}
for r in results:
    if r["year"] is not None:
        key = "1981-1989" if int(r["year"]) < 1990 else "2000-2019"
        by_era[key].append(r["cer_model"])
print("\nby era (Tesseract: 0.1650 old / 0.0688 modern):")
for era, vals in by_era.items():
    if vals:
        print(f"  {era}: {np.mean(vals):.4f} over {len(vals)} pages")

with open("test_predictions.jsonl", "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("\nper-page predictions written to test_predictions.jsonl")


# ══════════════════════════════════════════════════════════════
#  9. Look at real output before trusting the number
# ══════════════════════════════════════════════════════════════
# A CER can look respectable while the model quietly drops whole lines or
# repeats itself. Read the text.

for r in sorted(results, key=lambda r: r["cer_model"])[:2]:
    print(f"\n--- BEST page (CER {r['cer_model']:.4f}, year {r['year']}) ---")
    print("MODEL:", r["pred"][:400])
    print("GOLD :", normalize(test_ds[r["idx"]]["text"])[:400])

for r in sorted(results, key=lambda r: r["cer_model"])[-2:]:
    print(f"\n--- WORST page (CER {r['cer_model']:.4f}, year {r['year']}) ---")
    print("MODEL:", r["pred"][:400])
    print("GOLD :", normalize(test_ds[r["idx"]]["text"])[:400])
