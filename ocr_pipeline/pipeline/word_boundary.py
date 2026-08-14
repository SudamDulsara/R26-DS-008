"""
Word Boundary Detection using MuRIL
=====================================
Uses Google's MuRIL (Multilingual Representations for Indian Languages)
to detect whether adjacent Sinhala tokens belong to the same word
or are separate words.

Why MuRIL instead of mBERT:
  - Specifically trained on South Asian scripts including Sinhala
  - Better understanding of Sinhala morphology than general multilingual models
  - Smaller size (~500MB) with higher Sinhala accuracy

On first run this downloads the MuRIL model (~953MB total).
Subsequent runs use the cached version — loads in seconds.

Two-level approach:
  Level 1 — Dictionary check (fast):
    If merging two tokens produces a known Sinhala word → merge
  Level 2 — MuRIL similarity score (AI-powered):
    If not in dictionary, use MuRIL embeddings to score
    whether the tokens likely belong to the same word
    Threshold: 0.97 (high) to prevent over-merging
"""

import json
import re
import os
import torch
from transformers import AutoTokenizer, AutoModel

# ─────────────────────────────────────────────────────────────
#  Load Sinhala word list (built from Wikipedia corpus)
# ─────────────────────────────────────────────────────────────

WORDLIST_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data", "sinhala_wordlist.json"
)

def load_wordlist() -> set:
    """Load the Sinhala word list built from Wikipedia corpus."""
    try:
        with open(WORDLIST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"[word_boundary] Loaded {len(data):,} Sinhala words from dictionary")
        return set(data.keys())
    except FileNotFoundError:
        print(f"[word_boundary] Warning: wordlist not found at {WORDLIST_PATH}")
        print("[word_boundary] Run build_wordlist.py first to generate it")
        return set()

SINHALA_WORDS = load_wordlist()


# ─────────────────────────────────────────────────────────────
#  Load MuRIL model (cached after first download)
# ─────────────────────────────────────────────────────────────

MODEL_NAME = "google/muril-base-cased"
_tokenizer = None
_model     = None


def _load_model():
    """Load MuRIL tokenizer and model. Cached after first call."""
    global _tokenizer, _model
    if _tokenizer is None:
        print("[word_boundary] Loading MuRIL model (first time ~953MB download)...")
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        _model     = AutoModel.from_pretrained(MODEL_NAME)
        _model.eval()
        print("[word_boundary] MuRIL model loaded ✅")
    return _tokenizer, _model


# ─────────────────────────────────────────────────────────────
#  Helper functions
# ─────────────────────────────────────────────────────────────

def _is_sinhala(text: str) -> bool:
    """Return True if text contains Sinhala characters."""
    return bool(re.search(r'[\u0D80-\u0DFF]', text))


def _dict_score(token_a: str, token_b: str) -> float:
    """
    Check if merging two tokens produces a known Sinhala word.
    Returns 1.0 if merged form is in dictionary, 0.0 otherwise.
    """
    merged = token_a + token_b
    if merged in SINHALA_WORDS:
        return 1.0
    return 0.0


def _muril_score(token_a: str, token_b: str) -> float:
    """
    Use MuRIL embeddings to score whether two tokens belong
    to the same word.

    Method:
      - Get embedding of merged form:   token_a + token_b
      - Get embedding of separate form: token_a + " " + token_b
      - Compute cosine similarity between both embeddings
      - Higher similarity = tokens more likely belong to same word

    Returns float in [-1, 1]. Typical same-word score: > 0.97
    """
    tokenizer, model = _load_model()

    merged   = token_a + token_b
    separate = token_a + " " + token_b

    inputs_merged   = tokenizer(merged,   return_tensors="pt", truncation=True, max_length=128)
    inputs_separate = tokenizer(separate, return_tensors="pt", truncation=True, max_length=128)

    with torch.no_grad():
        out_merged   = model(**inputs_merged).last_hidden_state[:, 0, :]
        out_separate = model(**inputs_separate).last_hidden_state[:, 0, :]

    similarity = torch.nn.functional.cosine_similarity(
        out_merged, out_separate
    ).item()

    return similarity


# ─────────────────────────────────────────────────────────────
#  Word boundary decision
# ─────────────────────────────────────────────────────────────

def should_merge(token_a: str, token_b: str, use_muril: bool = True) -> bool:
    """
    Merge tokens ONLY if the combined form exists in the Sinhala dictionary.
    MuRIL scoring disabled — cosine similarity alone is not reliable
    for Sinhala word boundary detection without fine-tuning.
    """
    if not _is_sinhala(token_a) or not _is_sinhala(token_b):
        return False

    # Only merge if combined form is a confirmed Sinhala word
    return _dict_score(token_a, token_b) == 1.0


# ─────────────────────────────────────────────────────────────
#  Main word reconstruction function
# ─────────────────────────────────────────────────────────────

def reconstruct_words(text: str, use_mbert: bool = True) -> str:
    """
    Reconstruct proper Sinhala words from space-separated tokens.

    After Stage 1 Rules 1-5 fix grapheme structures, some tokens
    that belong to the same word are still separated by spaces.
    This function merges them back into correct Sinhala words.

    Parameters:
      text      : space-separated Sinhala tokens
      use_mbert : whether to use MuRIL for ambiguous cases
                  (parameter named use_mbert for backward compatibility)

    Returns:
      Reconstructed text with proper word boundaries
    """
    tokens = text.split()
    if len(tokens) <= 1:
        return text

    result = []
    i      = 0

    while i < len(tokens):
        current = tokens[i]

        # Keep merging forward while adjacent tokens belong to same word
        while i + 1 < len(tokens):
            next_token = tokens[i + 1]
            if should_merge(current, next_token, use_muril=use_mbert):
                current = current + next_token
                i += 1
            else:
                break

        result.append(current)
        i += 1

    return " ".join(result)