# transcribe.py

import os
import torch
import subprocess
import re

from transformers import pipeline
from corrections import COMMON_CORRECTIONS


# =========================================================
# CONFIG
# =========================================================

os.environ['HF_HOME'] = 'E:/huggingface_cache'

device = 0 if torch.cuda.is_available() else -1

model_id = "Lingalingeswaran/whisper-small-sinhala"


# =========================================================
# LOAD MODEL
# =========================================================

transcriber = pipeline(
    "automatic-speech-recognition",

    model=model_id,

    device=device,

    # Stable settings for Sinhala speech
    chunk_length_s=15,

    stride_length_s=5
)


# =========================================================
# AUDIO FILTERING
# =========================================================

def apply_filters(input_path):

    """
    Light filtering for Sinhala speech.
    Works with:
    - wav
    - flac
    - mp3
    """

    base_name = os.path.splitext(input_path)[0]

    filtered_path = f"{base_name}_filtered.wav"

    command = [

        "ffmpeg",

        "-y",

        "-i", input_path,

        # Light filtering
        "-af", "highpass=f=100, lowpass=f=7000",

        # Whisper preferred format
        "-ar", "16000",

        "-ac", "1",

        filtered_path
    ]

    try:

        subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        return filtered_path

    except Exception as e:

        print(f"❌ Filter Error: {e}")

        return input_path


# =========================================================
# TEXT CLEANING
# =========================================================

def clean_text(text):

    """
    Minimal cleanup without damaging Sinhala words.
    """

    text = str(text)

    # Remove whisper tags
    text = re.sub(r"<\|.*?\|>", "", text)

    # Remove broken replacement characters
    text = text.replace("�", "")

    # Remove extra spaces
    text = re.sub(r"\s+", " ", text)

    # Remove excessive repetition
    text = re.sub(r"(.)\1{5,}", r"\1", text)

    return text.strip()


# =========================================================
# RULE-BASED CORRECTION
# =========================================================

def apply_rule_based_corrections(text):

    words = text.split()

    corrected_words = []

    for word in words:

        # Exact dictionary correction
        if word in COMMON_CORRECTIONS:

            corrected_words.append(
                COMMON_CORRECTIONS[word]
            )

        else:

            corrected_words.append(word)

    corrected_text = " ".join(corrected_words)

    return corrected_text


# =========================================================
# MAIN TRANSCRIPTION
# =========================================================

def transcribe_audio(file_path):

    try:

        if not os.path.exists(file_path):

            return ""

        # -------------------------------------------------
        # STEP 1 — FILTER AUDIO
        # -------------------------------------------------

        clean_file = apply_filters(file_path)

        # -------------------------------------------------
        # STEP 2 — TRANSCRIBE
        # -------------------------------------------------

        result = transcriber(

            clean_file,

            generate_kwargs={

                "language": "sinhalese",

                "task": "transcribe",

                # deterministic decoding
                "do_sample": False
            }
        )

        text = result["text"]

        # -------------------------------------------------
        # STEP 3 — CLEAN TEXT
        # -------------------------------------------------

        text = clean_text(text)

        # -------------------------------------------------
        # STEP 4 — APPLY CORRECTIONS
        # -------------------------------------------------

        text = apply_rule_based_corrections(text)

        # -------------------------------------------------
        # STEP 5 — REMOVE TEMP FILE
        # -------------------------------------------------

        if clean_file != file_path and os.path.exists(clean_file):

            os.remove(clean_file)

        return text

    except Exception as e:

        print(f"❌ Error: {e}")

        return ""


# =========================================================
# CLI TESTING
# =========================================================

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to audio file"
    )

    args = parser.parse_args()

    print("\n⏳ Transcribing...")
    print("-" * 40)

    output = transcribe_audio(args.input)

    print("\n✅ FINAL OUTPUT:")
    print(output)