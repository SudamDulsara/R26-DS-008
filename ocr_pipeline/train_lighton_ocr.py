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

# Kaggle ships torchao 0.10.0. peft probes for it when building a LoRA layer
# and RAISES ImportError if the version is below 0.16 rather than simply
# reporting it as unavailable, which kills PeftModel.from_pretrained().
# Nothing here uses torchao, so remove it rather than chase a version.
#
# Checked through package metadata, not by importing it: importing would put
# it in sys.modules and peft might still find it after the uninstall.
try:
    from importlib.metadata import PackageNotFoundError, version as _pkg_version

    _tv = _pkg_version("torchao")
    if tuple(int(x) for x in _tv.split(".")[:2]) < (0, 16):
        print(f"removing incompatible torchao {_tv} (peft raises on it) ...")
        subprocess.run(
            [sys.executable, "-m", "pip", "uninstall", "-y", "-q", "torchao"],
            check=False,
        )
except PackageNotFoundError:
    pass
except Exception as _e:          # version string in an unexpected shape
    print(f"torchao version check skipped: {_e}")

# Check the model class exists before doing anything expensive.
try:
    from transformers import LightOnOcrForConditionalGeneration  # noqa: F401
    print("transformers already knows LightOnOcr")
except ImportError:
    print("upgrading transformers for LightOnOcr support ...")
    _pip("-U", "transformers")
    print("\n*** If the kernel restarts now, simply run this cell again. ***\n")
    from transformers import LightOnOcrForConditionalGeneration  # noqa: F401

import contextlib
import gc
import json
import math
import os
import random
import time
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
from peft import (
    LoraConfig,
    PeftModel,
    get_peft_model,
    prepare_model_for_kbit_training,
)
from PIL import Image
from transformers import (
    AutoConfig,
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

#: THE KEY MEASUREMENT BEHIND THE TWO LIMITS BELOW
#:
#: This tokenizer costs 1.41 tokens per Sinhala character -- close to
#: byte-level, because Qwen3's vocabulary has little Sinhala in it. Measured
#: over the 202 test pages with the real tokenizer:
#:
#:     characters per page   mean 1,609   max 3,198
#:     TOKENS per page       mean 2,265   median 2,131   p95 3,534   max 4,446
#:
#: Both limits were first set from a guess of ~1,200 tokens a page. That guess
#: was wrong by a factor of two and it broke both of them.

#: How many tokens the model may GENERATE when reading a page.
#:
#: Must clear the longest page outright. At 2048 -- the first value here --
#: about half of all pages would have stopped mid-sentence, and their CER
#: would have measured the cap rather than the model. 4608 clears the longest
#: page seen (4,446) with margin.
#:
#: Raising it costs nothing on normal pages: generation stops at the
#: end-of-sequence token, not at the cap.
MAX_TARGET_TOKENS = 4608

#: Ceiling on the WHOLE sequence: image tokens + prompt + page text.
#:
#: A page image is not free. At MAX_IMAGE_EDGE=1536 one page becomes 2,145
#: image tokens before a single character is counted, so a typical training
#: example is ~4,300 tokens.
#:
#: Measured share of pages that would be dropped, by cap:
#:
#:     image edge 1024 (962 img tokens):   4096 -> 12%    6144 -> 0%
#:     image edge 1536 (2145 img tokens):  4096 -> 70%    6144 -> 2%
#:     image edge 2048 (3848 img tokens):  4096 -> 100%   6144 -> 39%
#:
#: 4096 was the first value here and would have silently trained on 30% of
#: the corpus. 6144 keeps 98% of it.
#:
#: Pages over this are SKIPPED and counted, never truncated. Truncating cuts
#: into the image tokens and desynchronises them from the text -- which is
#: exactly how the first run died -- and it would violate this project's own
#: rule: never truncate silently, skip or flag and log every occurrence.
MAX_SEQ_TOKENS = 6144

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

#: How numbers are represented. Three modes, tried in this order by default.
#:
#:   name    weights   matmuls   notes
#:   ------  --------  --------  ------------------------------------------
#:   fp16    fp16      fp16      fastest; OVERFLOWED on this model (nan)
#:   amp     fp32      fp16      master weights fp32, autocast for matmuls;
#:                               softmax/layernorm/loss stay fp32, which is
#:                               where fp16 actually overflows
#:   fp32    fp32      fp32      slowest, always stable
#:
#: Why this matters more than it looks: the T4 runs fp32 at 8.1 TFLOPS but
#: fp16 on its tensor cores at 65. Falling all the way back to fp32 is not a
#: 2x slowdown, it can be closer to 8x -- the difference between a three-hour
#: run and most of a day. `amp` keeps the tensor cores while fixing the part
#: that actually overflows, so it is worth trying before giving them up.
#:
#: Measured on this model: plain fp16 gives a nan forward loss before any
#: training. Unquantising the vision encoder did NOT fix it, so the overflow
#: is inherent to fp16 activations here, not an artefact of 4-bit weights.
#:
#: PRECISION=auto (default) builds the model, checks whether one real page
#: produces a finite loss, and moves down the ladder until one does. Set it
#: explicitly to skip straight to a known-good mode on later runs.
#: `amp32` exists because both fp16 and amp returned nan, which does not
#: localise the overflow. The model has two numerically distinct halves:
#:
#:   - the vision encoder is NOT quantised, so autocast governs its dtype
#:   - the language model IS 4-bit, and bitsandbytes dequantises straight to
#:     bnb_4bit_compute_dtype regardless of what autocast says
#:
#: In `amp` both run in fp16, so a nan says nothing about which one broke.
#: `amp32` dequantises the language model in fp32 while still autocasting the
#: vision encoder to fp16. If it survives, the overflow was in the 4-bit path
#: and the vision half -- 24 layers over 2,145 image tokens, the expensive
#: part -- keeps its tensor cores.
PRECISION_LADDER = ["fp16", "amp", "amp32", "fp32"]

PRECISION = os.environ.get("PRECISION", "auto")
if PRECISION not in PRECISION_LADDER + ["auto"]:
    sys.exit(f"PRECISION must be auto, fp16, amp or fp32; got {PRECISION!r}")
print(f"precision: {PRECISION}")


def precision_spec(name):
    """(weight dtype, 4-bit compute dtype, use autocast) for a mode."""
    return {
        "fp16": (torch.float16, torch.float16, True),
        "amp": (torch.float32, torch.float16, True),
        "amp32": (torch.float32, torch.float32, True),
        "fp32": (torch.float32, torch.float32, False),
    }[name]

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
# On precision, and a correction: an earlier version of this comment argued
# that fp16 was safe here because the decoder is Qwen3-based, unlike the
# T5-family models whose bfloat16 activations overflowed fp16 and destroyed a
# ByT5 run. That reasoning was wrong -- it accounted for only half the model.
# The other half is a Pixtral vision encoder, which is Mistral-family and has
# exactly the bfloat16 heritage the argument was meant to rule out.
#
# The first smoke test duly returned validation loss nan. See PRECISION in
# section 2, the NO_QUANT list below, and the forward-pass check in section 6.

processor = AutoProcessor.from_pretrained(MODEL_NAME)

#: Parts that must NOT be quantised to 4 bits.
#:
#: The model is two towers (verified by inspecting its module tree):
#:     model.vision_encoder     PixtralVisionModel, 24 layers
#:     model.vision_projection  LightOnOcrMultiModalProjector
#:     model.language_model     Qwen3Model
#:
#: Crushing a vision encoder to 4 bits is the usual cause of nan in VLM QLoRA,
#: and it is also self-defeating here: reading Sinhala diacritics is precisely
#: a fine visual discrimination task. The encoder is a quarter of the model,
#: so leaving it in fp16 costs a few hundred MB and buys both stability and
#: accuracy. lm_head is left alone for the usual reason -- quantising the
#: output projection degrades generation quality out of proportion to its size.
NO_QUANT = ["vision_encoder", "vision_projection", "lm_head"]

CONFIG = AutoConfig.from_pretrained(MODEL_NAME)
IMAGE_TOKEN_ID = getattr(CONFIG, "image_token_id", None)

# Adapt the text decoder, not the vision encoder.
#
# This must be a REGEX, not a list of bare names. Both towers use the same
# leaf names -- q_proj, gate_proj and the rest appear in the Pixtral encoder
# as well as in Qwen3 -- and peft matches a plain list by name suffix. The
# earlier list-of-names version therefore attached LoRA to the vision encoder
# too, silently doing the opposite of what this comment claimed.
#
# Anchoring on `language_model` restricts adaptation to the decoder, which is
# where the real gap is: the encoder was trained to read documents and can
# already see the glyphs, but nothing in this model has ever had to EMIT
# Sinhala.
#
# If accuracy disappoints, adding the projector (linear_1, linear_2,
# merging_layer) is the next lever to try -- it is the bridge between what the
# encoder sees and what the decoder says.
LORA_TARGETS = (
    r".*language_model.*\.(q_proj|k_proj|v_proj|o_proj|gate_proj|up_proj|down_proj)"
)

def build_model(precision: str):
    """Load, quantise and attach LoRA at the given precision."""
    weight_dtype, compute_dtype, _ = precision_spec(precision)

    quant_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_use_double_quant=True,
        bnb_4bit_compute_dtype=compute_dtype,
        llm_int8_skip_modules=NO_QUANT,
    )

    m = LightOnOcrForConditionalGeneration.from_pretrained(
        MODEL_NAME,
        quantization_config=quant_config,
        dtype=weight_dtype,
        device_map={"": 0},
    )
    m = prepare_model_for_kbit_training(m, use_gradient_checkpointing=True)
    m.config.use_cache = False       # incompatible with gradient checkpointing

    m = get_peft_model(m, LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
        target_modules=LORA_TARGETS,
    ))

    # Verify the regex hit the decoder and nothing else. A silent miss here
    # would either train nothing or train the wrong tower.
    adapted = [n for n, _ in m.named_modules() if n.endswith("lora_A.default")]
    vision_hits = [n for n in adapted if "vision" in n]
    m.print_trainable_parameters()
    print(f"LoRA attached to {len(adapted)} modules; "
          f"{len(vision_hits)} of them in the vision encoder")
    if not adapted:
        sys.exit("LoRA matched nothing. Check LORA_TARGETS against module names.")
    if vision_hits:
        print("  WARNING: vision modules were adapted; the regex is too loose.")
    return m


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

    # No truncation, deliberately. The image expands into thousands of
    # placeholder tokens that must stay in exact correspondence with the
    # pixel data; cutting the sequence cuts into them and the processor
    # rejects the batch. Over-long pages are removed up front instead --
    # see the length scan below.
    enc = processor(
        images=images,
        text=fulls,
        return_tensors="pt",
        padding=True,
    )

    labels = enc["input_ids"].clone()

    pad_id = processor.tokenizer.pad_token_id
    if pad_id is not None:
        labels[labels == pad_id] = -100

    # Never train on the image placeholder tokens either -- they are inputs,
    # not something the model should learn to produce. Read from the config
    # rather than a model instance, so the collator can be built before any
    # model exists (the precision ladder below builds several).
    if IMAGE_TOKEN_ID is not None:
        labels[enc["input_ids"] == IMAGE_TOKEN_ID] = -100

    for i, plen in enumerate(prompt_lens):
        labels[i, :plen] = -100

    enc["labels"] = labels
    return enc


# Sanity-check one example before committing to a training run. If the target
# text is empty after masking, every gradient would be zero and the run would
# silently learn nothing.
_probe = collate([train_ds[0]])
_kept = (_probe["labels"] != -100).sum().item()
_total = _probe["input_ids"].shape[1]
print(f"probe example: {_total} tokens, {_kept} of them supervised "
      f"({_total - _kept} are image + prompt)")
if _kept == 0:
    sys.exit("Prompt masking removed every label. Fix build_texts() before training.")

# ── Build the model, at the fastest precision that survives ─────────
#
# One real page is pushed through the model and the loss is checked for being
# a finite number. Plain fp16 returns nan on this model -- measured, not
# assumed -- so the ladder drops to fp32 master weights with fp16 autocast,
# and only then to full fp32.
#
# Doing this here rather than after training is the whole point: the first
# attempt spent two minutes training before reporting nan, and full fp32 could
# cost most of a day for a run that never had to be that slow.


def probe_forward(m, precision) -> float:
    """Loss on one real page, computed the same way training will compute it."""
    _, _, use_amp = precision_spec(precision)
    batch = {k: v.to(m.device) for k, v in _probe.items()}
    ctx = (torch.autocast("cuda", dtype=torch.float16)
           if use_amp else contextlib.nullcontext())
    with torch.no_grad(), ctx:
        out = m(**batch)
    loss = float(out.loss)
    del out, batch
    gc.collect()
    torch.cuda.empty_cache()
    return loss


_candidates = PRECISION_LADDER if PRECISION == "auto" else [PRECISION]
model = None

for _p in _candidates:
    print(f"\n--- building model in {_p} ---")
    _candidate = build_model(_p)
    _loss = probe_forward(_candidate, _p)
    print(f"probe forward loss ({_p}): {_loss}")

    if math.isfinite(_loss):
        model = _candidate
        PRECISION = _p
        break

    print(f"  {_p} overflowed; discarding and trying the next mode")
    del _candidate
    gc.collect()
    torch.cuda.empty_cache()

if model is None:
    sys.exit(
        "\nEvery precision mode produced a non-finite loss, including fp32.\n"
        "That is not a precision problem -- something is wrong with the "
        "inputs or the masking. Inspect the probe batch before running again."
    )

DTYPE = precision_spec(PRECISION)[0]
USE_AMP = precision_spec(PRECISION)[2]
print(f"\n>>> training in {PRECISION} "
      f"(weights {DTYPE}, autocast {'on' if USE_AMP else 'off'})")


# ── How long is a page, in tokens? ──────────────────────────────────
#
# The vision encoder cuts the page into patches and merges neighbours, so the
# token count is the patch grid: ceil(w / stride) * ceil(h / stride), where
# stride = patch_size * spatial_merge_size. At 1536px that came to 2,145 for
# a single page -- more than the entire budget the first attempt allowed.
#
# The arithmetic is checked against the processor's actual output below rather
# than trusted, because a wrong stride would silently mis-filter the corpus.

def _cfg(obj, *names, default=None):
    for n in names:
        v = getattr(obj, n, None)
        if isinstance(v, int):
            return v
    return default


_patch = _cfg(getattr(CONFIG, "vision_config", object()), "patch_size", default=14)
_merge = _cfg(CONFIG, "spatial_merge_size", default=2)
_stride = _patch * _merge
print(f"vision: patch {_patch} x merge {_merge} -> {_stride}px per image token")


def shrunk_size(image):
    """The size shrink() would produce, without doing the resize."""
    w, h = image.size
    longest = max(w, h)
    if longest <= MAX_IMAGE_EDGE:
        return w, h
    scale = MAX_IMAGE_EDGE / longest
    return int(w * scale), int(h * scale)


def estimate_image_tokens(image) -> int:
    # Arithmetic only. Calling shrink() here would run a full LANCZOS resize
    # on all 808 pages purely to read two numbers off the result.
    w, h = shrunk_size(image)
    return -(-w // _stride) * -(-h // _stride)      # ceil division, both axes


_est = estimate_image_tokens(train_ds[0]["image"])
_actual_prompt = _total - _kept
print(f"image-token estimate {_est} vs measured prompt {_actual_prompt} "
      f"(difference is chat-template overhead)")
if abs(_est - _actual_prompt) > 100:
    print("  WARNING: estimate is far off. The length filter below may be wrong;"
          " check patch/merge sizes before trusting the skip count.")


def estimate_total_tokens(ex) -> int:
    text_tokens = len(processor.tokenizer(normalize(ex["text"]))["input_ids"])
    return estimate_image_tokens(ex["image"]) + text_tokens + 32   # +template


def drop_overlong(ds, name):
    """Remove pages that would exceed MAX_SEQ_TOKENS. Never truncate them."""
    lengths = [estimate_total_tokens(ds[i]) for i in range(len(ds))]
    keep = [i for i, n in enumerate(lengths) if n <= MAX_SEQ_TOKENS]
    dropped = len(ds) - len(keep)
    print(f"{name}: {len(ds)} pages, tokens min {min(lengths)} "
          f"median {int(np.median(lengths))} max {max(lengths)}")
    if dropped:
        over = [n for n in lengths if n > MAX_SEQ_TOKENS]
        print(f"  SKIPPED {dropped} page(s) over {MAX_SEQ_TOKENS} tokens "
              f"(longest {max(over)}) -- not truncated, removed")
    return ds.select(keep)


print("\nscanning page lengths ...")
train_ds = drop_overlong(train_ds, "train")
eval_ds = drop_overlong(eval_ds, "eval")
# The test split is NEVER filtered. Dropping hard pages from the held-out set
# would flatter the final CER against a baseline measured on all 202.


# ══════════════════════════════════════════════════════════════
#  7. Train
# ══════════════════════════════════════════════════════════════

# Convert the warmup fraction into an explicit step count.
#
# transformers 5.x deprecates warmup_ratio ("will be removed in v5.2"), and a
# silently ignored argument here would mean no warmup at all -- the opposite
# of the ByT5 problem but just as invisible. Computing it keeps the intent
# (5% of the run) while passing the argument that is actually supported.
_steps_per_epoch = math.ceil(len(train_ds) / (BATCH_SIZE * GRAD_ACCUM))
_total_steps = _steps_per_epoch * EPOCHS
WARMUP_STEPS = max(5, round(_total_steps * WARMUP_RATIO))
print(f"\nschedule: {_total_steps} optimiser steps over {EPOCHS} epoch(s), "
      f"{WARMUP_STEPS} of them warmup ({100 * WARMUP_STEPS / _total_steps:.0f}%)")


# ── How long will this actually take? ───────────────────────────────
#
# Measure one real forward+backward and extrapolate, rather than finding out
# four hours in. Falling back to fp32 costs a large multiple of fp16 on a
# Turing card, and at 1536px a page is ~4,300 tokens, so the two together can
# turn a three-hour plan into an overnight one.
#
# If the estimate is uncomfortable, MAX_IMAGE_EDGE is the lever with the most
# leverage -- image tokens dominate the sequence and attention cost grows with
# its square.

def time_one_step(m, precision) -> float:
    """Seconds for one page's forward + backward, as training will run it."""
    _, _, use_amp = precision_spec(precision)
    batch = {k: v.to(m.device) for k, v in _probe.items()}
    ctx = (torch.autocast("cuda", dtype=torch.float16)
           if use_amp else contextlib.nullcontext())
    m.train()
    torch.cuda.synchronize()
    t0 = time.time()
    with ctx:
        loss = m(**batch).loss
    loss.backward()
    torch.cuda.synchronize()
    elapsed = time.time() - t0
    m.zero_grad(set_to_none=True)
    del batch, loss
    gc.collect()
    torch.cuda.empty_cache()
    return elapsed


_sec_per_page = time_one_step(model, PRECISION)
_page_passes = len(train_ds) * EPOCHS
_train_hours = _sec_per_page * _page_passes / 3600
print(f"timing: {_sec_per_page:.1f}s per page in {PRECISION} at "
      f"{MAX_IMAGE_EDGE}px -> ~{_train_hours:.1f}h for {_page_passes:,} "
      f"page-passes (plus evaluation)")
if not SMOKE and _train_hours > 8:
    print("  WARNING: that is a long session. Lower MAX_IMAGE_EDGE or EPOCHS,")
    print("  or accept that this needs a full uninterrupted run.")

args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE,
    gradient_accumulation_steps=GRAD_ACCUM,
    learning_rate=LEARNING_RATE,
    num_train_epochs=EPOCHS,
    warmup_steps=WARMUP_STEPS,

    gradient_checkpointing=True,
    gradient_checkpointing_kwargs={"use_reentrant": False},

    eval_strategy="epoch",
    save_strategy="epoch",
    save_total_limit=2,
    load_best_model_at_end=True,
    metric_for_best_model="eval_loss",
    greater_is_better=False,

    # Autocast is what makes fp16 tensor cores usable without fp16 weights.
    # In "amp" mode the master weights stay fp32 while matmuls run in fp16;
    # in "fp32" mode this is off entirely.
    fp16=USE_AMP,
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

# Swap the quantised training model for merged fp16 weights before measuring.
#
# Each page needs ~2,265 generated tokens, and 4-bit inference through
# bitsandbytes is slow -- it trades speed for the memory that made training
# possible at all. Over 202 pages that difference is hours, not minutes.
#
# Folding the adapter into plain fp16 weights gives the same model
# mathematically, at roughly twice the speed. A 1B model in fp16 is ~2 GB and
# fits comfortably now that the optimiser state is gone.

del trainer, model
gc.collect()
torch.cuda.empty_cache()

print("\nmerging adapter into fp16 weights for evaluation ...")
_base = LightOnOcrForConditionalGeneration.from_pretrained(
    MODEL_NAME, dtype=DTYPE, device_map={"": 0},
)
infer_model = PeftModel.from_pretrained(_base, OUTPUT_DIR).merge_and_unload()
infer_model.eval()
infer_model.config.use_cache = True
print(f"eval model on {infer_model.device}, "
      f"{torch.cuda.memory_allocated() / 1e9:.1f} GB allocated")


@torch.no_grad()
def read_page(image):
    """Run the fine-tuned model over one page and return the text."""
    prompt_text = processor.apply_chat_template(
        PROMPT_MESSAGES, add_generation_prompt=True, tokenize=False
    )
    inputs = processor(
        images=[shrink(image)], text=[prompt_text], return_tensors="pt"
    ).to(infer_model.device)

    out = infer_model.generate(
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
