# =====================================================
# baseline_evaluation.py
#
# Evaluates the CURRENT Sinhala Whisper model
# on the held-out evaluation dataset.
#
# IMPORTANT:
# - Does NOT fine-tune the model
# - Does NOT modify the database
# - Does NOT modify train_clean.tsv
# - Does NOT use correct_text()
#
# =====================================================

import os
import csv
import re
import subprocess
import unicodedata

import torch

from transformers import pipeline

from jiwer import wer, cer


# =====================================================
# CONFIG
# =====================================================

os.environ["HF_HOME"] = "E:/huggingface_cache"

MODEL_ID = "Lingalingeswaran/whisper-small-sinhala"

EVAL_FILE = os.path.join(
    "dataset",
    "finetune",
    "whisper_eval.tsv"
)

device = (
    0
    if torch.cuda.is_available()
    else -1
)


# =====================================================
# LOAD WHISPER MODEL
# =====================================================

print()
print("=" * 70)
print("LOADING CURRENT SINHALA WHISPER MODEL")
print("=" * 70)

print()
print(f"Model: {MODEL_ID}")

if device == 0:

    print("Device: CUDA / GPU")

else:

    print("Device: CPU")


transcriber = pipeline(

    "automatic-speech-recognition",

    model=MODEL_ID,

    device=device,

    chunk_length_s=15,

    stride_length_s=5
)


print()
print("Model loaded successfully.")


# =====================================================
# AUDIO FILTERING
# =====================================================

def apply_filters(input_path):

    os.makedirs(
        "temp",
        exist_ok=True
    )

    filename = os.path.basename(
        input_path
    )

    filename = filename.replace(
        ".wav",
        "_baseline_filtered.wav"
    )

    filtered_path = os.path.join(
        "temp",
        filename
    )

    command = [

        "ffmpeg",

        "-y",

        "-i",
        input_path,

        "-af",
        "highpass=f=100,lowpass=f=7000",

        "-ar",
        "16000",

        "-ac",
        "1",

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

        print(
            f"Filter error: {e}"
        )

        return input_path


# =====================================================
# BASIC TEXT CLEANING
#
# This is NOT the correction system.
#
# It only removes obvious formatting artifacts
# so that WER/CER measurement is meaningful.
# =====================================================

def normalize_text(text):

    text = str(text)

    # ---------------------------------------------
    # Remove Whisper special tokens
    # ---------------------------------------------

    text = re.sub(
        r"<\|.*?\|>",
        "",
        text
    )

    # ---------------------------------------------
    # Remove replacement character
    # ---------------------------------------------

    text = text.replace(
        "�",
        ""
    )

    # ---------------------------------------------
    # Normalize Unicode
    # ---------------------------------------------

    text = unicodedata.normalize(
        "NFC",
        text
    )

    # ---------------------------------------------
    # Normalize whitespace
    # ---------------------------------------------

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


# =====================================================
# NORMALIZE FOR WER
#
# WER is word based.
# Remove punctuation so punctuation differences
# don't dominate the score.
# =====================================================

def normalize_for_wer(text):

    text = normalize_text(
        text
    )

    characters = []

    for char in text:

        category = unicodedata.category(
            char
        )

        # Keep letters, numbers and whitespace
        if (
            category.startswith("L")
            or category.startswith("N")
            or category.startswith("M")
            or char.isspace()
        ):

            characters.append(char)

        else:

            # Replace punctuation/symbols
            # with whitespace
            characters.append(" ")

    text = "".join(
        characters
    )

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


# =====================================================
# NORMALIZE FOR CER
#
# CER is character based.
# Spaces are removed.
# =====================================================

def normalize_for_cer(text):

    text = normalize_for_wer(
        text
    )

    text = text.replace(
        " ",
        ""
    )

    return text


# =====================================================
# TRANSCRIBE ONE AUDIO FILE
# =====================================================

def transcribe_file(audio_path):

    if not os.path.exists(
        audio_path
    ):

        print(
            f"❌ Audio not found: {audio_path}"
        )

        return ""

    filtered_file = apply_filters(
        audio_path
    )

    try:

        result = transcriber(

            filtered_file,

            generate_kwargs={

                "language": "sinhalese",

                "task": "transcribe",

                "do_sample": False
            }
        )

        text = result["text"]

        return normalize_text(
            text
        )

    except Exception as e:

        print(
            f"❌ Transcription error: {e}"
        )

        return ""

    finally:

        # -----------------------------------------
        # Delete temporary filtered audio
        # -----------------------------------------

        if (

            filtered_file != audio_path

            and

            os.path.exists(
                filtered_file
            )

        ):

            try:

                os.remove(
                    filtered_file
                )

            except Exception:
                pass


# =====================================================
# LOAD EVALUATION DATASET
# =====================================================

def load_evaluation_dataset():

    rows = []

    with open(

        EVAL_FILE,

        "r",

        encoding="utf-8"

    ) as f:

        reader = csv.reader(

            f,

            delimiter="\t"
        )

        for line_number, row in enumerate(

            reader,

            start=1
        ):

            if not row:
                continue

            if len(row) != 3:

                raise ValueError(

                    f"Invalid row {line_number}: "
                    f"expected 3 fields, "
                    f"found {len(row)}"
                )

            audio_path = row[0].strip()

            transcript = row[1].strip()

            duration = row[2].strip()

            rows.append(

                {

                    "audio_path": audio_path,

                    "reference": transcript,

                    "duration": duration
                }
            )

    return rows


# =====================================================
# MAIN EVALUATION
# =====================================================

def main():

    print()
    print("=" * 70)
    print("BASELINE EVALUATION")
    print("=" * 70)

    print()
    print(
        f"Evaluation file: {EVAL_FILE}"
    )

    rows = load_evaluation_dataset()

    print()
    print(
        f"Evaluation clips: {len(rows)}"
    )

    # -------------------------------------------------
    # Store references and predictions
    # -------------------------------------------------

    references = []

    predictions = []

    successful = 0

    failed = 0

    # -------------------------------------------------
    # Process clips
    # -------------------------------------------------

    for i, row in enumerate(

        rows,

        start=1
    ):

        audio_path = row["audio_path"]

        reference = row["reference"]

        print()
        print("-" * 70)

        print(
            f"[{i}/{len(rows)}]"
        )

        print(
            f"Audio: {audio_path}"
        )

        print(
            f"Reference: {reference}"
        )

        # ---------------------------------------------
        # Transcribe
        # ---------------------------------------------

        prediction = transcribe_file(
            audio_path
        )

        print(
            f"Prediction: {prediction}"
        )

        # ---------------------------------------------
        # Check result
        # ---------------------------------------------

        if not prediction:

            print(
                "❌ Failed to generate transcription"
            )

            failed += 1

            continue

        successful += 1

        references.append(
            normalize_for_wer(
                reference
            )
        )

        predictions.append(
            normalize_for_wer(
                prediction
            )
        )

    # =================================================
    # RESULTS
    # =================================================

    print()
    print("=" * 70)
    print("BASELINE RESULTS")
    print("=" * 70)

    print()
    print(
        f"Total clips       : {len(rows)}"
    )

    print(
        f"Successful        : {successful}"
    )

    print(
        f"Failed            : {failed}"
    )

    if not references:

        print()
        print(
            "No successful transcriptions."
        )

        return

    # -------------------------------------------------
    # WER
    # -------------------------------------------------

    baseline_wer = wer(

        references,

        predictions
    )

    # -------------------------------------------------
    # CER
    # -------------------------------------------------

    cer_references = [

        normalize_for_cer(
            text
        )

        for text in references
    ]

    cer_predictions = [

        normalize_for_cer(
            text
        )

        for text in predictions
    ]

    baseline_cer = cer(

        cer_references,

        cer_predictions
    )

    # -------------------------------------------------
    # Display
    # -------------------------------------------------

    print()
    print(
        f"Baseline WER : "
        f"{baseline_wer:.4f}"
    )

    print(
        f"Baseline WER : "
        f"{baseline_wer * 100:.2f}%"
    )

    print()

    print(
        f"Baseline CER : "
        f"{baseline_cer:.4f}"
    )

    print(
        f"Baseline CER : "
        f"{baseline_cer * 100:.2f}%"
    )

    print()
    print("=" * 70)

    print(
        "BASELINE EVALUATION COMPLETED"
    )

    print("=" * 70)


# =====================================================
# RUN
# =====================================================

if __name__ == "__main__":

    main()