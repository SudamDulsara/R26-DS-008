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
#  0. One GPU. This must be the first executable line in the file.
# ══════════════════════════════════════════════════════════════
# Kaggle's "GPU T4 x2" gives two cards. The Trainer sees two, wraps the model
# in DataParallel, and copies it to the second card on every step.
#
# That is fatal here, not merely wasteful: a 4-bit quantised layer holds
# pointers into the memory of the card it was created on. DataParallel copies
# the module to card 1 without moving what those pointers refer to, so the
# copy reads addresses belonging to card 0 and CUDA aborts with
#
#     torch.AcceleratorError: CUDA error: an illegal memory access
#     ... in peft/tuners/lora/bnb.py, forward
#
# observed 2026-08-18 in the vision encoder's feed-forward LoRA.
#
# WHY THIS MOVED TO THE TOP OF THE FILE
# -------------------------------------
# It used to sit next to the torch import in section 2, which looked correct
# -- the rule is "set it before torch is imported". But section 1 checks that
# transformers knows LightOnOcr, and importing transformers imports torch.
# So torch was already loaded by the time the variable was set, CUDA counted
# two devices, and the pin did nothing.
#
# Nothing may execute above this line. In a notebook, also make sure no
# earlier cell imports torch or transformers.
import os

os.environ["CUDA_VISIBLE_DEVICES"] = "0"

# Let the allocator grow its segments instead of fragmenting them.
#
# This also used to live in section 2 and also needs to be here. Gradient
# checkpointing recomputes activations during the backward pass, so it
# allocates and frees a stream of tensors of constantly changing sizes. Left
# to the default allocator those freed blocks are the wrong shape to reuse,
# and the reserved pool grows without the memory being available:
#
#     OutOfMemoryError: Tried to allocate 1.41 GiB. ... 1.12 GiB is free.
#     Of the allocated memory 7.55 GiB is allocated by PyTorch, and
#     5.75 GiB is reserved by PyTorch but unallocated.
#
# 5.75 GB held and unusable on a 14.5 GB card, observed on a T4 2026-08-18.
#
# BOTH names are set. PyTorch renamed this to PYTORCH_ALLOC_CONF; the old
# name is deprecated and, on the version Kaggle now ships, apparently ignored
# -- the OOM message asks for the new name even though the old one was set.
#
# Windows supports neither and prints "expandable_segments not supported on
# this platform", so do not ask for it there.
if os.name != "nt":
    os.environ["PYTORCH_ALLOC_CONF"] = "expandable_segments:True"
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

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

# bitsandbytes is OPTIONAL and its absence is not fatal.
#
# It provides 4-bit quantisation, which is what squeezed this model onto a
# 16 GB T4. On a card with bfloat16 the model fits in ~2 GB unquantised, so
# quantisation buys headroom rather than feasibility -- and bitsandbytes is
# the single most fragile dependency here, especially on a new GPU
# architecture or on Windows. Failing to install it should cost the run some
# memory headroom, not the whole evening.
# The import is guarded and expected to fail on some machines, so editors
# flagging it as unresolved are correct and unhelpful at the same time --
# hence the type: ignore. It is a real import rather than a find_spec check
# because a broken install (wrong CUDA version, missing DLL) is present on
# disk but raises when imported, which is exactly the case worth catching.
try:
    _ensure("bitsandbytes")
    import bitsandbytes as _bnb                      # type: ignore
    HAVE_BNB = True
    print(f"bitsandbytes {getattr(_bnb, '__version__', 'unknown')}")
except Exception as _exc:
    HAVE_BNB = False
    print(f"bitsandbytes unavailable ({type(_exc).__name__}: {_exc}); "
          f"running without 4-bit quantisation")

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

# CUDA_VISIBLE_DEVICES and the allocator config are both set in section 0, at
# the very top of the file. Neither can live here: section 1 imports
# transformers, which imports torch, so by this point torch has already
# counted the cards and read its allocator settings.

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

#: BASE_ONLY=1 skips training entirely and scores the UNTUNED model.
#:
#: Nobody knows what LightOnOCR does on Sinhala out of the box. Its card lists
#: 11 languages and Sinhala is not one of them, and the paper reports only
#: 1.05% AFTER their fine-tune. The number before it has never been published.
#:
#: It is worth an hour to find out, because it decides how much work the rest
#: of this project needs:
#:
#:   base is already good     -> Pipeline B works with no training at all, and
#:                               the fine-tune becomes an optional improvement
#:   base is poor             -> "untuned X vs fine-tuned Y" is exactly the
#:                               evidence that justifies the fine-tuning step
#:
#: Either answer is useful; not knowing is the only bad state. Pair it with
#: EVAL_LIMIT to keep it to a sensible length.
BASE_ONLY = os.environ.get("BASE_ONLY", "0") == "1"
if BASE_ONLY:
    print("*** BASE_ONLY -- no training, scoring the untuned model ***")

#: EVAL_ONLY=1 skips training and scores an adapter that ALREADY EXISTS.
#:
#: Why this is needed: a real run is capped at EVAL_LIMIT=60 pages because
#: generating a full page is slow and a Kaggle session has a deadline. But
#: Tesseract's 0.1079 is the mean over all 202, so a 60-page number is
#: indicative rather than quotable. Getting the comparable figure used to
#: require repeating the whole fine-tune -- hours of GPU for a measurement.
#:
#: With this, a finished adapter is loaded and scored directly. Inference
#: only (5.2 GB in fp32, 3.2 GB in bf16), so an 8 GB laptop can do all 202
#: overnight without touching the Kaggle quota.
#:
#:     EVAL_ONLY=1  EVAL_LIMIT=0  ADAPTER=results/lightonocr-sinhala-acts
#:
#: EVAL_LIMIT=0 means all 202 pages. ADAPTER defaults to OUTPUT_DIR.
EVAL_ONLY = os.environ.get("EVAL_ONLY", "0") == "1"
ADAPTER_DIR = os.environ.get("ADAPTER", OUTPUT_DIR)
if EVAL_ONLY:
    if BASE_ONLY:
        sys.exit("BASE_ONLY and EVAL_ONLY are mutually exclusive: one scores "
                 "the model WITHOUT an adapter, the other scores one WITH.")
    if not os.path.isdir(ADAPTER_DIR):
        sys.exit(f"EVAL_ONLY=1 but no adapter folder at {ADAPTER_DIR!r}.")
    print(f"*** EVAL_ONLY -- no training, scoring {ADAPTER_DIR} ***")

#: True when section 7 must not train. Both modes measure an existing model.
SKIP_TRAINING = BASE_ONLY or EVAL_ONLY

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
#:
#: BACK TO 1536 after run 1 (2026-08-17).
#:
#: Run 1 used 1024 (~88 DPI) on the theory that it would roughly halve the
#: time. It saved 13%, not 50% -- cost tracks total sequence tokens, and the
#: sequence only shrank 4,646 -> 3,445 because the text half is unchanged.
#: So the resolution was given up almost for nothing.
#:
#: And it appears to have cost accuracy. Run 1 scored 0.3312 CER producing
#: fluent but wrong Sinhala with perfect layout and correct numerals -- large
#: shapes survived 88 DPI, the small marks that carry meaning in this script
#: did not.
#:
#: 1536 is ~131 DPI and sits at the vision encoder's own native image_size of
#: 1540, so it is the most this model was built to use.
MAX_IMAGE_EDGE = 1536

#: 4-bit quantisation: "auto", "4bit" or "none".
#:
#: "auto" uses 4-bit when bitsandbytes is importable and skips it otherwise.
#:
#: What quantisation is actually for here: it made a 1B vision-language model
#: trainable on a 16 GB T4 alongside fp32 activations. On a bf16-capable card
#: the whole model is ~2 GB unquantised, so 4-bit becomes a way to buy
#: activation headroom rather than a requirement.
#:
#: Worth turning off deliberately if VRAM allows, because unquantised weights
#: skip the dequantisation step on every matmul and remove the project's most
#: fragile dependency. On an 8 GB card at 1536px it is close either way -- try
#: "none" first, and fall back to "4bit" if it runs out of memory.
QUANT = os.environ.get("QUANT", "auto")
if QUANT not in ("auto", "4bit", "none"):
    sys.exit(f"QUANT must be auto, 4bit or none; got {QUANT!r}")
USE_4BIT = HAVE_BNB if QUANT == "auto" else (QUANT == "4bit")
if USE_4BIT and not HAVE_BNB:
    sys.exit("QUANT=4bit but bitsandbytes did not import. Use QUANT=none.")
print(f"quantisation: {'4-bit' if USE_4BIT else 'none (full precision weights)'}")

BATCH_SIZE = 1          # one page per step; pages are large

#: How many pages are averaged into one weight update.
#:
#: This does NOT change how long the run takes. Wall-clock time is set by the
#: number of pages pushed through the model, and that is epochs x 707 whatever
#: this is. What it changes is how many times the weights actually move:
#:
#:     accum   updates per epoch   effective batch
#:     8       89                  8
#:     4       177                 4
#:     2       354                 2
#:
#: That matters when the budget only allows one epoch. At 42.1s per page on a
#: T4, 707 pages is 8.3 hours, so a Kaggle session fits one epoch and no more.
#: One epoch at accum 8 is 89 updates -- too few for LoRA to learn a script
#: the model has never seen. Dropping to 4 doubles the updates for free.
#:
#: The cost is a noisier gradient. 4 is the compromise: still an average over
#: four pages, twice the learning steps.
GRAD_ACCUM = int(os.environ.get("GRAD_ACCUM", "8"))
LEARNING_RATE = 1e-4    # standard for LoRA; the base weights stay frozen

#: MEASURED, not estimated. At 1024px in fp32 on a T4: 15.8s per page.
#:
#:     epochs   steps   training   + evaluation
#:     2        178     6.2 h      ~7 h
#:     3        267     9.3 h      ~10 h      <- fits a Kaggle session
#:     4        356     12.4 h     ~13 h      <- does not
#:
#: Dropping the image edge from 1536 to 1024 saved only 13% (18.2s -> 15.8s),
#: not the ~50% first assumed. Image tokens fell by more than half, but the
#: sequence only shrank 4,646 -> 3,445 tokens, and the language model works
#: across the whole sequence. Cost tracks total tokens, not image tokens.
#:
#: 3 epochs is the most that fits with room for the evaluation pass. The
#: adapter is saved before evaluation begins, so an overrun would cost the
#: measurement but not the model.
#:
#: Settable from the environment (EPOCHS=2) because the affordable number
#: depends on the machine, and Kaggle kills a session at 12 hours whether or
#: not the adapter has been saved. At the T4's measured 18.2s per page,
#: 2 epochs is ~7h of training and 3 is ~11h -- the second does not leave room
#: to score the test set.
EPOCHS = int(os.environ.get("EPOCHS", "3"))
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
#: vision encoder to fp16.
#:
#: `mixed` is the mirror image: vision encoder in fp32 (no autocast, so it is
#: never pushed to fp16), language model dequantised to fp16.
#:
#: RESULT, all five measured on a T4 on 2026-08-16:
#:
#:     mode    vision   language model   probe loss
#:     ------  -------  ---------------  ----------
#:     fp16    fp16     fp16             nan
#:     amp     fp16     fp16             nan
#:     amp32   fp16     fp32             nan
#:     mixed   fp32     fp16             nan
#:     fp32    fp32     fp32             0.8904
#:
#: Read the middle two rows together: amp32 puts only the vision encoder in
#: fp16 and fails; mixed puts only the language model in fp16 and also fails.
#: BOTH halves overflow independently, so there is no split that rescues the
#: tensor cores. On this card fp32 is the only option, and it costs roughly
#: 18s per page against a ~4s fp16 equivalent.
#:
#: The real fix is a card with bfloat16 -- any RTX 30/40, L4, A100. bf16 has
#: fp32's exponent range at fp16's speed, and this whole ladder becomes
#: unnecessary. The T4 is Turing and predates it.
#: `bf16` is the answer on any card that supports it, and it is tried first
#: when one is present. bfloat16 has fp32's exponent range with fp16's speed,
#: so the overflow that defeats every fp16 mode above simply does not occur,
#: and the tensor cores are still used. Support starts at Ampere (RTX 30) and
#: includes Ada (RTX 40), Blackwell (RTX 50), L4, A100, H100.
#:
#: The T4 in Kaggle is Turing and predates it, which is the entire reason this
#: ladder exists.
_ALL_MODES = ["bf16", "fp16", "amp", "amp32", "mixed", "fp32"]
PRECISION_LADDER = ["fp16", "amp", "amp32", "mixed", "fp32"]

def has_real_bf16() -> bool:
    """
    True only if the GPU has bfloat16 in HARDWARE.

    torch.cuda.is_bf16_supported() is not the right question: it returns True
    when bf16 can be *emulated*, and it duly claimed yes on a Tesla T4, which
    is Turing and has no bf16 units at all. Emulated bf16 gives none of the
    speed, which is the entire reason to want it.

    Compute capability 8.0 (Ampere) is where hardware bf16 starts. That covers
    A100, RTX 30xx, L4, RTX 40xx (8.9), RTX 50xx (12.x), H100. Turing is 7.5.
    """
    if not torch.cuda.is_available():
        return False
    major, _minor = torch.cuda.get_device_capability()
    return major >= 8


if has_real_bf16():
    PRECISION_LADDER = ["bf16"] + PRECISION_LADDER
    _cap = torch.cuda.get_device_capability()
    print(f"GPU compute capability {_cap[0]}.{_cap[1]} -- has hardware "
          f"bfloat16, trying it first")

PRECISION = os.environ.get("PRECISION", "auto")
if PRECISION not in _ALL_MODES + ["auto"]:
    sys.exit(f"PRECISION must be auto or one of {_ALL_MODES}; got {PRECISION!r}")
print(f"precision: {PRECISION}")


def precision_spec(name):
    """(weight dtype, 4-bit compute dtype, use fp16 autocast) for a mode."""
    return {
        "bf16": (torch.bfloat16, torch.bfloat16, False),
        "fp16": (torch.float16, torch.float16, True),
        "amp": (torch.float32, torch.float16, True),
        "amp32": (torch.float32, torch.float32, True),
        "mixed": (torch.float32, torch.float16, False),
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

# Did the pin in section 0 actually take? Print it rather than assume it.
#
# The failure is silent otherwise: the Trainer builds DataParallel, the run
# looks fine for several minutes, and then dies inside a quantised LoRA layer
# with an error that says nothing about multiple GPUs.
_N_GPU = torch.cuda.device_count()
print(f"visible GPUs: {_N_GPU}")
if _N_GPU > 1:
    print("  WARNING: more than one GPU is visible, so CUDA_VISIBLE_DEVICES "
          "did not take effect.")
    print("  Something imported torch before section 0 ran -- in a notebook, "
          "check the cells above.")
    print("  Section 7 forces single-GPU training anyway, but fix the cause.")


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

# Adapt BOTH towers and the bridge between them.
#
# This must be a REGEX, not a list of bare names: both towers use the same
# leaf names (q_proj, gate_proj and so on appear in the Pixtral encoder as
# well as in Qwen3) and peft matches a plain list by suffix, so a list cannot
# express "these, in that tower".
#
# WHY THIS CHANGED (2026-08-17, after run 1 scored 0.3312 CER)
# ------------------------------------------------------------
# The first version anchored on `language_model` only, arguing that the
# encoder "already reads printed glyphs well" and only the decoder needed to
# learn to emit Sinhala. That was an assumption, and the run disproved it.
#
# What run 1 actually produced was fluent, well-formed, WRONG Sinhala: layout,
# line breaks, numerals and punctuation all correct, individual words wrong.
# That is the signature of a decoder that learned the language perfectly and
# is guessing words that fit, because the visual features reaching it do not
# distinguish one Sinhala glyph from another.
#
# Corroborating it: Tesseract is 2.4x worse on 1980s scans than modern print,
# but run 1 was uniformly bad across both eras (0.360 vs 0.319). A model
# limited by paper quality tracks paper quality. One limited by its own
# perception does not.
#
# So the vision path is now adapted too:
#
#   vision_encoder     24 Pixtral layers -- learns to see Sinhala glyphs
#   vision_projection  linear_1, linear_2, merging_layer -- the bridge from
#                      what the encoder sees to what the decoder reads
#   language_model     as before
#
# This roughly triples the trainable parameters, which on a LoRA budget is
# still around 3% of the model.
LORA_TARGETS = (
    r".*(language_model|vision_encoder|vision_projection).*\."
    r"(q_proj|k_proj|v_proj|o_proj|gate_proj|up_proj|down_proj"
    r"|linear_1|linear_2|merging_layer)"
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
    ) if USE_4BIT else None

    m = LightOnOcrForConditionalGeneration.from_pretrained(
        MODEL_NAME,
        quantization_config=quant_config,
        dtype=weight_dtype,
        device_map={"": 0},
    )

    if USE_4BIT:
        m = prepare_model_for_kbit_training(m, use_gradient_checkpointing=True)
    else:
        # prepare_model_for_kbit_training() is specifically for quantised
        # models -- it casts norms to fp32 and freezes the base. Unquantised,
        # do the two things that actually matter by hand: freeze everything
        # (LoRA re-enables its own parameters) and turn on checkpointing.
        for param in m.parameters():
            param.requires_grad = False
        m.gradient_checkpointing_enable(
            gradient_checkpointing_kwargs={"use_reentrant": False}
        )
        m.enable_input_require_grads()

    m.config.use_cache = False       # incompatible with gradient checkpointing

    m = get_peft_model(m, LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
        target_modules=LORA_TARGETS,
    ))

    # Verify the regex hit all three parts. A silent miss trains the wrong
    # thing, and run 1 showed what that costs: adapting only the decoder gave
    # fluent, confident, wrong Sinhala at 0.3312 CER.
    adapted = [n for n, _ in m.named_modules() if n.endswith("lora_A.default")]
    enc = sum("vision_encoder" in n for n in adapted)
    proj = sum("vision_projection" in n for n in adapted)
    lang = sum("language_model" in n for n in adapted)
    m.print_trainable_parameters()
    print(f"LoRA attached to {len(adapted)} modules: "
          f"{enc} vision encoder, {proj} projector, {lang} language model")
    if not adapted:
        sys.exit("LoRA matched nothing. Check LORA_TARGETS against module names.")
    for part, count in (("vision encoder", enc), ("projector", proj),
                        ("language model", lang)):
        if count == 0:
            print(f"  WARNING: nothing adapted in the {part}. If that is not "
                  f"deliberate, LORA_TARGETS is wrong.")
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
USE_BF16 = PRECISION == "bf16"
print(f"\n>>> training in {PRECISION} "
      f"(weights {DTYPE}, fp16 autocast {'on' if USE_AMP else 'off'}, "
      f"bf16 {'on' if USE_BF16 else 'off'})")


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

def time_one_step(m, precision, warmup=1, runs=2) -> float:
    """
    Seconds for one page's forward + backward, as training will run it.

    The first call is discarded. CUDA compiles and autotunes kernels on first
    use, so an unwarmed measurement reads roughly twice the sustained rate --
    40.8s against a true 18.2s when this was first added, which would have
    doubled every estimate built on it.
    """
    _, _, use_amp = precision_spec(precision)
    ctx = (torch.autocast("cuda", dtype=torch.float16)
           if use_amp else contextlib.nullcontext())

    # Match the Trainer's checkpointing settings before timing anything.
    #
    # prepare_model_for_kbit_training() enables gradient checkpointing with
    # PyTorch's default (reentrant) implementation, while TrainingArguments
    # re-enables it with use_reentrant=False. The two recompute different
    # amounts of the graph, and the difference is not small: this function
    # read 42.7s per page against the Trainer's actual 18.25s, inflating the
    # projected run from 10.7 hours to 25.1 and nearly changing the decision
    # about what settings to use.
    try:
        m.gradient_checkpointing_enable(
            gradient_checkpointing_kwargs={"use_reentrant": False}
        )
    except TypeError:                      # older transformers
        m.gradient_checkpointing_enable()

    m.train()

    def one():
        batch = {k: v.to(m.device) for k, v in _probe.items()}
        torch.cuda.synchronize()
        t0 = time.time()
        with ctx:
            loss = m(**batch).loss
        loss.backward()
        torch.cuda.synchronize()
        dt = time.time() - t0
        m.zero_grad(set_to_none=True)
        del batch, loss
        return dt

    for _ in range(warmup):
        one()
    timings = [one() for _ in range(runs)]

    gc.collect()
    torch.cuda.empty_cache()
    return min(timings)


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

    # Exactly one of these may be on. bf16 is preferred wherever the card
    # supports it; fp16 autocast is the fallback that keeps tensor cores when
    # the weights are fp32; both are off in plain fp32 mode.
    fp16=USE_AMP,
    bf16=USE_BF16,

    logging_steps=5,     # ~265 steps total, so log often
    report_to="none",
    seed=SEED,
    remove_unused_columns=False,   # the collator needs `image` and `text`

    # ZERO WORKERS ON WINDOWS -- this is not a tuning choice, it is a bug fix.
    #
    # Dataloader workers are separate processes. On Linux they are forked, so
    # they inherit the parent and re-run nothing. Windows has no fork: Python
    # SPAWNS them, and a spawned worker re-imports the main module from the
    # top. This script does all its work at module level, so every worker
    # re-executed the whole thing -- re-downloading, rebuilding the model and
    # loading another 2 GB copy onto the GPU.
    #
    # Observed on an 8 GB RTX 5050: all output printed three times, the timing
    # probe read 691s per page instead of ~5, and the run died with
    # "CUDA error: out of memory" in backward.
    #
    # The alternative fix is to wrap everything in `if __name__ == "__main__"`,
    # but this file is deliberately a flat script that can be pasted into a
    # Kaggle cell, and that guard does not work in a notebook. Zero workers
    # costs little here anyway: at batch size 1 the GPU step dominates, and
    # the collator's work is one image resize.
    dataloader_num_workers=0 if os.name == "nt" else 2,
)

# Second lock on the single-GPU rule, independent of the environment variable.
#
# TrainingArguments counts the cards at construction time and the Trainer
# wraps the model in DataParallel whenever that count is above one. Setting it
# back to one here works no matter what happened during import, so the run
# cannot be destroyed by an import-order accident in a notebook.
if getattr(args, "_n_gpu", 1) > 1:
    print(f"forcing single-GPU training (Trainer had counted {args._n_gpu})")
    args._n_gpu = 1

if SKIP_TRAINING:
    _why = "the untuned model" if BASE_ONLY else f"the adapter in {ADAPTER_DIR}"
    print(f"\nskipping training. Section 8 scores {_why}.")
    trainer = None
else:
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

# Swap the 4-bit training model for unquantised weights before measuring.
#
# Each page needs ~2,265 generated tokens, and 4-bit inference through
# bitsandbytes is slow -- it trades speed for the memory that made training
# possible at all. Over 202 pages that difference is hours, not minutes.
# Folding the adapter into plain weights gives the same model mathematically,
# at better speed, and the optimiser state is gone so there is room.
#
# The dtype is DTYPE, whatever the precision ladder settled on. On a T4 that
# is fp32, because generation overflows in fp16 for the same reason training
# does -- it is the same forward pass.

del trainer, model
gc.collect()
torch.cuda.empty_cache()

_base = LightOnOcrForConditionalGeneration.from_pretrained(
    MODEL_NAME, dtype=DTYPE, device_map={"": 0},
)

if BASE_ONLY:
    # No adapter exists. Score the model exactly as it comes off the Hub.
    print(f"\nloading UNTUNED {MODEL_NAME} in {PRECISION} for evaluation ...")
    infer_model = _base
else:
    print(f"\nmerging adapter from {ADAPTER_DIR} into {PRECISION} weights ...")
    infer_model = PeftModel.from_pretrained(_base, ADAPTER_DIR).merge_and_unload()

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

_what = "UNTUNED base model" if BASE_ONLY else "This fine-tune"
print(f"\n== HELD-OUT TEST SET ({len(results)} pages) ==")
print(f"Tesseract        : {TESSERACT_BASELINE:.4f} CER   (measured 2026-08-16)")
print(f"{_what:17}: {model_cer:.4f} CER")
print(f"Paper's target   : 0.0105 CER   (same model, same data, their QLoRA run)")
if EVAL_LIMIT:
    print(f"  NOTE: {len(results)} pages, not all 202. The Tesseract figure above is")
    print("  the mean over all 202, so this comparison is indicative, not quotable.")

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

_outfile = "test_predictions_base.jsonl" if BASE_ONLY else "test_predictions.jsonl"
with open(_outfile, "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print(f"\nper-page predictions written to {_outfile}")


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
