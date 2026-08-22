# =====================================================
# main.py
# CONTINUOUS DATASET PIPELINE
# =====================================================

import sys

sys.stdout.reconfigure(encoding="utf-8")


# =====================================================
# IMPORTS
# =====================================================

from database import create_database
from collector import get_next_video

from download import download_audio
from preprocess import preprocess_audio
from segment import split_audio
from transcribe_dataset import transcribe_dataset
from dataset_writer import write_dataset


# =====================================================
# MAIN
# =====================================================

def main():

    # -------------------------------------------------
    # CREATE DATABASE
    # -------------------------------------------------

    create_database()

    print("\n" + "=" * 70)
    print("SINHALA CONTINUOUS DATASET PIPELINE")
    print("=" * 70)

    # -------------------------------------------------
    # STEP 1
    # AUTOMATIC VIDEO COLLECTION
    # -------------------------------------------------

    print("\nSearching YouTube automatically...")

    video = get_next_video()

    # -------------------------------------------------
    # CHECK VIDEO
    # -------------------------------------------------

    if video is None:

        print("\nNo new video available.")

        return

    # -------------------------------------------------
    # STEP 2
    # DOWNLOAD AUDIO
    # -------------------------------------------------

    print("\nDownloading audio...")

    audio_path = download_audio(video)

    if not audio_path:

        print("\n❌ Audio download failed.")

        return

    # -------------------------------------------------
    # STEP 3
    # PREPROCESS
    # -------------------------------------------------

    print("\nPreprocessing audio...")

    clean_audio = preprocess_audio(
        audio_path
    )

    if not clean_audio:

        print("\n❌ Audio preprocessing failed.")

        return

    # -------------------------------------------------
    # STEP 4
    # SEGMENT
    # -------------------------------------------------

    print("\nSegmenting audio...")

    chunks = split_audio(
        clean_audio,
        video
    )

    if len(chunks) == 0:

        print("\nNo valid chunks generated.")

        return

    print(
        f"\nChunks Generated: {len(chunks)}"
    )

    # -------------------------------------------------
    # STEP 5
    # TRANSCRIBE
    # -------------------------------------------------

    print("\nTranscribing with fine-tuned Sinhala Whisper V2...")

    results = transcribe_dataset(
        chunks,
        video
    )

    if len(results) == 0:

        print("\nNo valid transcripts generated.")

        return

    print(
        f"\nTranscripts Generated: {len(results)}"
    )

    # -------------------------------------------------
    # STEP 6
    # SAVE DATASET
    # -------------------------------------------------

    print("\nSaving dataset...")

    write_dataset(
        results
    )

    # -------------------------------------------------
    # FINISHED
    # -------------------------------------------------

    print()

    print("=" * 70)

    print(
        "PIPELINE FINISHED SUCCESSFULLY"
    )

    print("=" * 70)

    print(
        f"\nVideo : {video['title']}"
    )

    print(
        f"Chunks Generated : {len(chunks)}"
    )

    print(
        f"Dataset Records : {len(results)}"
    )

    print(
        "\nDatabase : Video saved "
        "(automatic collection)"
    )


# =====================================================
# ENTRY
# =====================================================

if __name__ == "__main__":

    main()