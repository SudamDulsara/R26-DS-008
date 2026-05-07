import os
import torch
import subprocess
import re
from transformers import pipeline

# ---------------- CONFIG ----------------
os.environ['HF_HOME'] = 'E:/huggingface_cache'

device = 0 if torch.cuda.is_available() else -1
model_id = "Lingalingeswaran/whisper-small-sinhala"

# IMPORTANT: smaller chunk + smaller stride = less hallucination
transcriber = pipeline(
    "automatic-speech-recognition",
    model=model_id,
    device=device,
    chunk_length_s=15,     # 🔥 reduced from 30
    stride_length_s=5      # 🔥 reduced from 10
)

# ---------------- AUDIO FILTER ----------------
def apply_filters(input_path):
    """
    Mild filtering — keeps Sinhala clarity while removing noise
    """
    filtered_path = input_path.replace(".wav", "_filtered.wav")

    command = [
        "ffmpeg", "-y", "-i", input_path,
        "-af", "highpass=f=100, lowpass=f=7000",
        "-ar", "16000",
        "-ac", "1",
        filtered_path
    ]

    try:
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return filtered_path
    except Exception:
        return input_path


# ---------------- TEXT CLEANING ----------------
def clean_text(text):
    """
    Remove hallucinated tokens + garbage
    """
    # remove whisper tokens like <|transcribe|>
    text = re.sub(r"<\|.*?\|>", "", text)

    # remove weird repeated symbols
    text = re.sub(r"[^\u0D80-\u0DFFa-zA-Z0-9\s.,!?]", "", text)

    # collapse repeated characters
    text = re.sub(r"(.)\1{3,}", r"\1", text)

    return text.strip()


# ---------------- MAIN TRANSCRIBE ----------------
def transcribe_audio(file_path):
    try:
        if not os.path.exists(file_path):
            return ""

        # Step 1: filter audio
        clean_file = apply_filters(file_path)

        # Step 2: simple decoding (NO forcing)
        result = transcriber(
            clean_file,
            generate_kwargs={
                "language": "sinhalese",
                "task": "transcribe",
                "do_sample": False   # deterministic
            }
        )

        text = result["text"]

        # Step 3: clean output
        text = clean_text(text)

        # Step 4: remove temp file
        if clean_file != file_path and os.path.exists(clean_file):
            os.remove(clean_file)

        return text

    except Exception as e:
        print(f"❌ Error: {e}")
        return ""


# ---------------- CLI ----------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    args = parser.parse_args()

    print("⏳ Transcribing...")
    print("-" * 30)

    output = transcribe_audio(args.input)

    print(output)