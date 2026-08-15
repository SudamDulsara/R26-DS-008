# =========================================================
# transcribe.py
# =========================================================

import os
import torch
import subprocess
import re

from transformers import pipeline

from corrections import correct_text

# =========================================================
# CONFIG
# =========================================================

os.environ["HF_HOME"] = "E:/huggingface_cache"

device = 0 if torch.cuda.is_available() else -1

# =========================================================
# MODEL
# =========================================================

model_id = os.path.join(
    "models",
    "final_whisper_sinhala_v1"
)

print("Using fine-tuned Whisper V1:")
print(os.path.abspath(model_id))

# =========================================================
# LOAD MODEL
# =========================================================

transcriber = pipeline(
    "automatic-speech-recognition",
    model=model_id,
    device=device,
    chunk_length_s=15,
    stride_length_s=5
)

# =========================================================
# AUDIO FILTERING
# =========================================================

def apply_filters(input_path):

    # --------------------------------------------
    # TEMP FOLDER
    # --------------------------------------------

    os.makedirs("temp", exist_ok=True)

    # --------------------------------------------
    # CREATE TEMP FILE NAME
    # --------------------------------------------

    filename = os.path.basename(input_path)

    filename = filename.replace(
        ".wav",
        "_filtered.wav"
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

        print(f"❌ Filter Error: {e}")

        return input_path

# =========================================================
# CLEAN TEXT
# =========================================================

def clean_text(text):

    text = str(text)

    text = re.sub(
        r"<\|.*?\|>",
        "",
        text
    )

    text = text.replace(
        "�",
        ""
    )

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    text = re.sub(
        r"(.)\1{5,}",
        r"\1",
        text
    )

    return text.strip()

# =========================================================
# MAIN TRANSCRIPTION FUNCTION
# =========================================================

def transcribe_audio(file_path):

    try:

        # --------------------------------------------
        # CHECK FILE
        # --------------------------------------------

        if not os.path.exists(file_path):

            return ""

        # --------------------------------------------
        # APPLY FILTERS
        # --------------------------------------------

        clean_file = apply_filters(file_path)

        # --------------------------------------------
        # TRANSCRIBE
        # --------------------------------------------

        result = transcriber(

            clean_file,

            generate_kwargs={

                "language": "si",

                "task": "transcribe",

                "do_sample": False

            }

        )

        text = result["text"]

        # --------------------------------------------
        # CLEAN TEXT
        # --------------------------------------------

        text = clean_text(text)

        # --------------------------------------------
        # CORRECT TEXT
        # --------------------------------------------

        text = correct_text(text)

        # --------------------------------------------
        # DELETE TEMP FILE
        # --------------------------------------------

        if (

            clean_file != file_path

            and

            os.path.exists(clean_file)

        ):

            os.remove(clean_file)

        return text

    except Exception as e:

        print(f"❌ Error: {e}")

        return ""

# =========================================================
# CLI TEST
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