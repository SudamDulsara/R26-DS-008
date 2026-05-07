# main.py

import os
import time
import csv
import re
import yt_dlp

from segment import split_audio
from transcribe import transcribe_audio


# =========================================================
# CONFIG
# =========================================================

DATASET_DIR = "dataset"

CLIPS_DIR = "dataset/clips"

SAVE_PATH = "dataset/train.tsv"

os.makedirs(DATASET_DIR, exist_ok=True)

os.makedirs(CLIPS_DIR, exist_ok=True)


# =========================================================
# DOWNLOAD AUDIO
# =========================================================

def download_audio(url):

    print(f"\n📥 Downloading: {url}")

    temp_filename = f"temp_audio_{int(time.time())}"

    ydl_opts = {

        'format': 'bestaudio/best',

        'noplaylist': True,

        'postprocessors': [{

            'key': 'FFmpegExtractAudio',

            'preferredcodec': 'wav',

            'preferredquality': '192',
        }],

        'outtmpl': f"{temp_filename}.%(ext)s",

        'quiet': True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:

        ydl.download([url])

    return f"{temp_filename}.wav"


# =========================================================
# FINAL TEXT CLEANING
# =========================================================

def clean_text(text):

    if not text:
        return ""

    text = text.strip()

    text = text.replace("\n", " ")

    # collapse repeated chars
    text = re.sub(r'(.)\1{2,}', r'\1', text)

    # collapse spaces
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


# =========================================================
# DATASET SAVE
# =========================================================

def save_dataset(rows):

    file_exists = os.path.isfile(SAVE_PATH)

    with open(
        SAVE_PATH,
        "a",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(

            f,

            fieldnames=[
                "path",
                "text",
                "duration"
            ],

            delimiter="\t"
        )

        if not file_exists:

            writer.writeheader()

        writer.writerows(rows)


# =========================================================
# MAIN PIPELINE
# =========================================================

def main():

    video_url = input("\n🎥 Enter YouTube URL: ")

    # -----------------------------------------------------
    # DOWNLOAD AUDIO
    # -----------------------------------------------------

    try:

        audio_file = download_audio(video_url)

    except Exception as e:

        print(f"\n❌ Download Failed: {e}")

        return

    # -----------------------------------------------------
    # SEGMENT AUDIO
    # -----------------------------------------------------

    print("\n✂️ Segmenting Audio...")

    chunks = split_audio(audio_file)

    if not chunks:

        print("\n❌ No valid chunks created")

        return

    # -----------------------------------------------------
    # TRANSCRIBE
    # -----------------------------------------------------

    dataset_rows = []

    seen_texts = set()

    for i, chunk in enumerate(chunks):

        print(f"\n[{i+1}/{len(chunks)}] 🎤 Transcribing: {chunk}")

        raw_text = transcribe_audio(chunk)

        final_text = clean_text(raw_text)

        # -------------------------------------------------
        # REMOVE INVALID TRANSCRIPTIONS
        # -------------------------------------------------

        if not final_text:

            print(f"❌ Invalid transcription")

            # delete bad chunk
            if os.path.exists(chunk):
                os.remove(chunk)

            continue

        # -------------------------------------------------
        # REMOVE DUPLICATES
        # -------------------------------------------------

        if final_text in seen_texts:

            print("⚠ Duplicate skipped")

            if os.path.exists(chunk):
                os.remove(chunk)

            continue

        seen_texts.add(final_text)

        # -------------------------------------------------
        # SAVE VALID ROW
        # -------------------------------------------------

        dataset_rows.append({

            "path": chunk,

            "text": final_text,

            "duration": 10.0
        })

        print(f"✅ VALID: {final_text}")

    # -----------------------------------------------------
    # SAVE DATASET
    # -----------------------------------------------------

    if dataset_rows:

        save_dataset(dataset_rows)

        print(f"\n💾 Saved {len(dataset_rows)} rows")

    else:

        print("\n⚠ No valid rows to save")

    # -----------------------------------------------------
    # CLEAN TEMP AUDIO
    # -----------------------------------------------------

    if os.path.exists(audio_file):

        os.remove(audio_file)

    print("\n🚀 Pipeline Complete!")


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    main()