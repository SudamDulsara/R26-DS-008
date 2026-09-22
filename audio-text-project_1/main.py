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
from export_dataset import export_dataset


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

    print(
        "\nTranscribing with fine-tuned Sinhala Whisper V2..."
    )

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
    # PIPELINE FINISHED
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

    # -------------------------------------------------
    # ASK USER TO DOWNLOAD DATASET
    # -------------------------------------------------

    print()

    print("=" * 70)

    print(
        "DATASET DOWNLOAD"
    )

    print("=" * 70)

    print(
        "\nDo you want to download the generated "
        "dataset to your PC?"
    )

    while True:

        choice = input(
            "\nEnter Y for Yes or N for No: "
        ).strip().lower()

        if choice in ("y", "yes"):

            print(
                "\nExporting dataset..."
            )

            export_dataset()

            break

        elif choice in ("n", "no"):

            print(
                "\nDataset download skipped."
            )

            break

        else:

            print(
                "Please enter Y or N."
            )


# =====================================================
# ENTRY
# =====================================================

if __name__ == "__main__":

    main()