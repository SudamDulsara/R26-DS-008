# =====================================================
# export_dataset.py
# Export SQLite Sinhala Dataset to ZIP
# =====================================================

import sqlite3
import csv
import zipfile
from pathlib import Path


# =====================================================
# PROJECT PATHS
# =====================================================

# This file is inside the project folder.
PROJECT_DIR = Path(__file__).resolve().parent


# =====================================================
# DATABASE
# =====================================================

# Database is outside the project folder:
#
# E:\R26-DS-008\data\inbox\videos.db
#
# The path is calculated automatically relative to
# the project folder.

DATABASE_PATH = (
    PROJECT_DIR.parent
    / "data"
    / "inbox"
    / "videos.db"
)


# =====================================================
# EXPORT FOLDER
# =====================================================

# Temporary/exported dataset files remain inside
# the project folder before creating the ZIP.

EXPORT_DIR = (
    PROJECT_DIR
    / "dataset_export"
)


# =====================================================
# AUDIO FOLDER
# =====================================================

AUDIO_DIR = (
    EXPORT_DIR
    / "audio"
)


# =====================================================
# METADATA CSV
# =====================================================

CSV_FILE = (
    EXPORT_DIR
    / "metadata.csv"
)


# =====================================================
# WINDOWS DOWNLOADS FOLDER
# =====================================================

# Automatically find the current Windows user's
# Downloads folder.
#
# Example:
# C:\Users\John\Downloads
#
# No username is hard-coded.

DOWNLOADS_DIR = (
    Path.home()
    / "Downloads"
)


# =====================================================
# FINAL ZIP FILE
# =====================================================

ZIP_FILE = (
    DOWNLOADS_DIR
    / "Sinhala_Dataset.zip"
)


# =====================================================
# USER CONFIRMATION
# =====================================================

def confirm_export():

    print("\n")
    print("=" * 70)
    print("SINHALA DATASET EXPORT")
    print("=" * 70)

    print(
        "\nThe dataset will be exported and saved as:"
    )

    print(
        f"\n{ZIP_FILE}"
    )

    print(
        "\nThe ZIP will contain:"
    )

    print(
        "  • Audio WAV files"
    )

    print(
        "  • Metadata CSV"
    )

    print(
        "  • Transcriptions"
    )

    print(
        "  • Word-level timestamps"
    )

    print(
        "  • Topic classifications"
    )

    print(
        "\nDo you want to download/export the dataset "
        "to your Downloads folder?"
    )

    while True:

        choice = input(
            "\nEnter Y for Yes or N for No: "
        ).strip().lower()

        if choice in ("y", "yes"):

            return True

        if choice in ("n", "no"):

            return False

        print(
            "Please enter Y or N."
        )


# =====================================================
# EXPORT FUNCTION
# =====================================================

def export_dataset():

    # -------------------------------------------------
    # ASK USER BEFORE EXPORT
    # -------------------------------------------------

    if not confirm_export():

        print("\nDataset export cancelled.")

        return None


    print("\n")
    print("=" * 70)
    print("EXPORTING SINHALA DATASET")
    print("=" * 70)


    # =================================================
    # DISPLAY PATHS
    # =================================================

    print("\nDatabase:")
    print(DATABASE_PATH)

    print("\nTemporary export location:")
    print(EXPORT_DIR)

    print("\nWindows Downloads folder:")
    print(DOWNLOADS_DIR)

    print("\nFinal Dataset ZIP:")
    print(ZIP_FILE)


    # =================================================
    # CHECK DATABASE
    # =================================================

    if not DATABASE_PATH.exists():

        raise FileNotFoundError(
            f"Database not found: {DATABASE_PATH}"
        )


    # =================================================
    # CREATE EXPORT FOLDERS
    # =================================================

    EXPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    AUDIO_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    DOWNLOADS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )


    # =================================================
    # CONNECT DATABASE
    # =================================================

    connection = sqlite3.connect(
        DATABASE_PATH,
        timeout=30
    )

    cursor = connection.cursor()


    try:

        # =================================================
        # GET ALL CLIPS
        # =================================================

        cursor.execute(
            """
            SELECT
                clip_id,
                video_id,
                audio,
                topic,
                transcription,
                word_timestamps,
                start_time,
                end_time,
                duration,
                verified,
                language,
                created_at
            FROM clips
            ORDER BY created_at ASC
            """
        )

        rows = cursor.fetchall()


        print(
            f"\nDatabase records found: {len(rows)}"
        )


        # =================================================
        # CHECK RECORDS
        # =================================================

        if not rows:

            raise RuntimeError(
                "No dataset records found in the database."
            )


        # =================================================
        # CREATE CSV
        # =================================================

        fieldnames = [

            "clip_id",

            "video_id",

            "audio_file",

            "topic",

            "transcription",

            "word_timestamps",

            "start_time",

            "end_time",

            "duration",

            "verified",

            "language",

            "created_at"
        ]


        successful = 0
        failed = 0


        # =================================================
        # OPEN CSV
        # =================================================

        with open(
            CSV_FILE,
            "w",
            newline="",
            encoding="utf-8-sig"
        ) as csv_file:

            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames
            )

            writer.writeheader()


            # =================================================
            # EXPORT EACH CLIP
            # =================================================

            for index, row in enumerate(
                rows,
                start=1
            ):

                (
                    clip_id,
                    video_id,
                    audio_blob,
                    topic,
                    transcription,
                    word_timestamps,
                    start_time,
                    end_time,
                    duration,
                    verified,
                    language,
                    created_at
                ) = row


                print(
                    f"\n[{index}/{len(rows)}] {clip_id}"
                )


                # =============================================
                # CHECK AUDIO
                # =============================================

                if not audio_blob:

                    print(
                        "  ❌ Audio BLOB missing."
                    )

                    failed += 1

                    continue


                # =============================================
                # AUDIO FILE NAME
                # =============================================

                audio_filename = (
                    f"{clip_id}.wav"
                )


                audio_path = (
                    AUDIO_DIR
                    / audio_filename
                )


                # =============================================
                # EXPORT AUDIO
                # =============================================

                try:

                    with open(
                        audio_path,
                        "wb"
                    ) as audio_file:

                        audio_file.write(
                            audio_blob
                        )


                    print(
                        f"  ✓ Audio exported "
                        f"({len(audio_blob):,} bytes)"
                    )


                except Exception as e:

                    print(
                        f"  ❌ Audio export failed: {e}"
                    )

                    failed += 1

                    continue


                # =============================================
                # WRITE METADATA
                # =============================================

                writer.writerow(
                    {

                        "clip_id":
                            clip_id,

                        "video_id":
                            video_id,

                        "audio_file":
                            f"audio/{audio_filename}",

                        "topic":
                            topic,

                        "transcription":
                            transcription,

                        "word_timestamps":
                            word_timestamps,

                        "start_time":
                            start_time,

                        "end_time":
                            end_time,

                        "duration":
                            duration,

                        "verified":
                            verified,

                        "language":
                            language,

                        "created_at":
                            created_at
                    }
                )


                successful += 1


    finally:

        # =================================================
        # CLOSE DATABASE
        # =================================================

        connection.close()


    # =================================================
    # CREATE ZIP
    # =================================================

    print("\n")
    print("-" * 70)
    print("CREATING DATASET ZIP")
    print("-" * 70)


    # =================================================
    # REMOVE PREVIOUS ZIP
    # =================================================

    if ZIP_FILE.exists():

        print(
            "\nRemoving previous dataset ZIP..."
        )

        ZIP_FILE.unlink()


    # =================================================
    # CREATE ZIP
    # =================================================

    with zipfile.ZipFile(
        ZIP_FILE,
        "w",
        compression=zipfile.ZIP_DEFLATED
    ) as zip_file:


        # =============================================
        # ADD AUDIO FILES
        # =============================================

        audio_files = list(
            AUDIO_DIR.glob("*.wav")
        )


        print(
            f"\nAudio files to include: "
            f"{len(audio_files)}"
        )


        for audio_file in audio_files:

            zip_file.write(
                audio_file,
                arcname=(
                    f"audio/"
                    f"{audio_file.name}"
                )
            )


        # =============================================
        # ADD CSV
        # =============================================

        zip_file.write(
            CSV_FILE,
            arcname="metadata.csv"
        )


    # =================================================
    # VERIFY ZIP
    # =================================================

    if not ZIP_FILE.exists():

        raise RuntimeError(
            "Dataset ZIP was not created."
        )


    zip_size = (
        ZIP_FILE.stat().st_size
    )


    # =================================================
    # VERIFY ZIP CONTENTS
    # =================================================

    with zipfile.ZipFile(
        ZIP_FILE,
        "r"
    ) as zip_file:

        zip_contents = (
            zip_file.namelist()
        )


        audio_count = sum(
            1
            for item in zip_contents
            if item.startswith("audio/")
            and item.endswith(".wav")
        )


        metadata_exists = (
            "metadata.csv"
            in zip_contents
        )


    # =================================================
    # FINAL OUTPUT
    # =================================================

    print("\n" + "=" * 70)
    print("DATASET EXPORT COMPLETED")
    print("=" * 70)


    print(
        f"\nDatabase records : "
        f"{len(rows)}"
    )


    print(
        f"Successful       : "
        f"{successful}"
    )


    print(
        f"Failed           : "
        f"{failed}"
    )


    print(
        f"\nAudio files      : "
        f"{audio_count}"
    )


    print(
        f"Metadata CSV     : "
        f"{'✓ Present' if metadata_exists else '❌ Missing'}"
    )


    print(
        f"\nTemporary audio folder:"
    )


    print(
        AUDIO_DIR
    )


    print(
        f"\nMetadata CSV:"
    )


    print(
        CSV_FILE
    )


    print(
        f"\nDataset ZIP:"
    )


    print(
        ZIP_FILE
    )


    print(
        f"\nZIP size: "
        f"{zip_size:,} bytes"
    )


    print(
        "\n✓ Sinhala_Dataset.zip created successfully."
    )


    print(
        "\n✓ The dataset has been saved to your "
        "Windows Downloads folder."
    )


    print("=" * 70)


    return ZIP_FILE


# =====================================================
# RUN DIRECTLY
# =====================================================

if __name__ == "__main__":

    export_dataset()