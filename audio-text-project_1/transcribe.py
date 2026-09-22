# =========================================================
# transcribe.py
# =========================================================

import os
import torch
import subprocess
import re
import traceback

from transformers import pipeline
from corrections import correct_text


# =========================================================
# CONFIGURATION
# =========================================================

os.environ["HF_HOME"] = "E:/huggingface_cache"

device = 0 if torch.cuda.is_available() else -1

model_id = os.path.join(
    "models",
    "final_whisper_sinhala_v2"
)

print("Using fine-tuned Whisper V2:")
print(os.path.abspath(model_id))


# =========================================================
# HUGGING FACE WHISPER
# =========================================================

transcriber = pipeline(
    "automatic-speech-recognition",
    model=model_id,
    device=device
)


# =========================================================
# AUDIO FILTERING
# =========================================================

def apply_filters(input_path):

    os.makedirs(
        "temp",
        exist_ok=True
    )

    filename = os.path.basename(
        input_path
    ).replace(
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

        print(
            f"❌ Filter Error: {e}"
        )

        return input_path


# =========================================================
# TEXT CLEANING
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
# WORD CLEANING
# =========================================================

def clean_timestamp_word(word):

    word = str(
        word or ""
    )

    word = re.sub(
        r"<\|.*?\|>",
        "",
        word
    )

    word = word.replace(
        "�",
        ""
    )

    return word.strip()


# =========================================================
# EXTRACT WORD TIMESTAMPS
# =========================================================

def extract_word_timestamps(result):

    chunks = result.get(
        "chunks",
        []
    )

    word_timestamps = []

    for chunk in chunks:

        word = clean_timestamp_word(
            chunk.get(
                "text",
                ""
            )
        )

        timestamp = chunk.get(
            "timestamp"
        )

        if not word:
            continue

        if not timestamp:
            continue

        if len(timestamp) != 2:
            continue

        start, end = timestamp

        if start is None:
            start = 0.0

        if end is None:
            continue

        try:

            start = float(start)
            end = float(end)

        except (
            TypeError,
            ValueError
        ):

            continue

        if end < start:
            continue

        word_timestamps.append(
            {
                "word": word,
                "start": round(
                    start,
                    3
                ),
                "end": round(
                    end,
                    3
                )
            }
        )

    return word_timestamps


# =========================================================
# FORMAT WORD TIMESTAMPS
# =========================================================

def format_word_timestamps(word_timestamps):

    lines = []

    for item in word_timestamps:

        word = item.get(
            "word",
            ""
        ).strip()

        start = item.get(
            "start"
        )

        end = item.get(
            "end"
        )

        if not word:
            continue

        if start is None or end is None:
            continue

        lines.append(
            f"{word} - "
            f"{start:.2f}s-"
            f"{end:.2f}s"
        )

    return "\n".join(
        lines
    )


# =========================================================
# TRANSCRIBE AUDIO
# =========================================================

def transcribe_audio(file_path):

    clean_file = file_path

    try:

        # -------------------------------------------------
        # Check input
        # -------------------------------------------------

        if not os.path.exists(
            file_path
        ):

            print(
                f"❌ Audio file not found: {file_path}"
            )

            return None

        # -------------------------------------------------
        # Apply audio filtering
        # -------------------------------------------------

        clean_file = apply_filters(
            file_path
        )

        # -------------------------------------------------
        # HUGGING FACE WHISPER
        # WORD-LEVEL TIMESTAMPS
        # -------------------------------------------------

        print(
            "\n🔍 Generating transcript with word timestamps..."
        )

        try:

            result = transcriber(
                clean_file,
                return_timestamps="word",
                generate_kwargs={
                    "language": "si",
                    "task": "transcribe",
                    "eos_token_id": 50257
                }
            )

        except Exception:

            print(
                "\n❌ FULL HUGGING FACE ERROR:"
            )

            print(
                "=" * 70
            )

            traceback.print_exc()

            print(
                "=" * 70
            )

            raise

        # -------------------------------------------------
        # Get transcript
        # -------------------------------------------------

        raw_text = result.get(
            "text",
            ""
        )

        raw_text = clean_text(
            raw_text
        )

        # -------------------------------------------------
        # Get word timestamps
        # -------------------------------------------------

        word_timestamps = extract_word_timestamps(
            result
        )

        if not word_timestamps:

            raise RuntimeError(
                "Whisper did not return word-level timestamps."
            )

        # -------------------------------------------------
        # Existing correction system
        # -------------------------------------------------

        text = correct_text(
            raw_text
        )

        # -------------------------------------------------
        # Remove temporary filtered file
        # -------------------------------------------------

        if (
            clean_file != file_path
            and os.path.exists(
                clean_file
            )
        ):

            os.remove(
                clean_file
            )

        # -------------------------------------------------
        # Return result
        # -------------------------------------------------

        return {
            "text": text,
            "word_timestamps": word_timestamps
        }

    except Exception as e:

        print(
            f"\n❌ Transcription Error: {e}"
        )

        if (
            clean_file != file_path
            and os.path.exists(
                clean_file
            )
        ):

            try:

                os.remove(
                    clean_file
                )

            except Exception:

                pass

        return None


# =========================================================
# TEST MODE
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

    print(
        "\n⏳ Transcribing..."
    )

    print(
        "-" * 40
    )

    output = transcribe_audio(
        args.input
    )

    print(
        "\n✅ FINAL OUTPUT:"
    )

    if output:

        print(
            "\nTranscript:"
        )

        print(
            output["text"]
        )

        print(
            "\nWord timestamps:"
        )

        print(
            format_word_timestamps(
                output["word_timestamps"]
            )
        )

    else:

        print(
            "❌ Transcription failed."
        )