# =====================================================
# split_dataset.py
# CREATE WHISPER FINE-TUNING TRAIN / EVALUATION SPLIT
# =====================================================

import os
import csv
import random


# =====================================================
# CONFIG
# =====================================================

INPUT_FILE = os.path.join(
    "dataset",
    "train_clean.tsv"
)

OUTPUT_DIR = os.path.join(
    "dataset",
    "finetune"
)

TRAIN_FILE = os.path.join(
    OUTPUT_DIR,
    "whisper_train.tsv"
)

EVAL_FILE = os.path.join(
    OUTPUT_DIR,
    "whisper_eval.tsv"
)

# Fixed seed = same split every time
RANDOM_SEED = 42

# 80% training / 20% evaluation
TRAIN_RATIO = 0.80


# =====================================================
# LOAD CLEAN DATASET
# =====================================================

def load_dataset():

    rows = []

    print()
    print("Loading:")
    print(INPUT_FILE)

    with open(
        INPUT_FILE,
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

            # -----------------------------------------
            # Skip completely empty rows
            # -----------------------------------------

            if not row:
                continue

            if len(row) == 1 and not row[0].strip():
                continue

            # -----------------------------------------
            # Every row must contain:
            #
            # audio_path
            # transcript
            # duration
            # -----------------------------------------

            if len(row) != 3:

                raise ValueError(
                    f"\nInvalid row {line_number}.\n"
                    f"Expected 3 fields but found "
                    f"{len(row)} fields.\n"
                    f"Row: {row}"
                )

            audio_path = row[0].strip()
            transcript = row[1].strip()
            duration = row[2].strip()

            # -----------------------------------------
            # Validate audio path
            # -----------------------------------------

            if not audio_path:

                raise ValueError(
                    f"Missing audio path "
                    f"at row {line_number}"
                )

            # -----------------------------------------
            # Validate transcript
            # -----------------------------------------

            if not transcript:

                raise ValueError(
                    f"Missing transcript "
                    f"at row {line_number}"
                )

            # -----------------------------------------
            # Validate duration
            # -----------------------------------------

            if not duration:

                raise ValueError(
                    f"Missing duration "
                    f"at row {line_number}"
                )

            rows.append(
                (
                    audio_path,
                    transcript,
                    duration
                )
            )

    return rows


# =====================================================
# CHECK DUPLICATES
# =====================================================

def check_duplicates(rows):

    audio_paths = [
        row[0]
        for row in rows
    ]

    duplicates = (
        len(audio_paths)
        != len(set(audio_paths))
    )

    if duplicates:

        raise ValueError(
            "Duplicate audio paths found "
            "in train_clean.tsv."
        )


# =====================================================
# SAVE TSV
# =====================================================

def save_tsv(
    filename,
    rows
):

    with open(
        filename,
        "w",
        encoding="utf-8",
        newline=""
    ) as f:

        writer = csv.writer(
            f,
            delimiter="\t",
            lineterminator="\n"
        )

        for row in rows:

            writer.writerow(row)


# =====================================================
# MAIN
# =====================================================

def main():

    print()
    print("=" * 70)
    print("WHISPER FINE-TUNING DATASET SPLIT")
    print("=" * 70)

    # -------------------------------------------------
    # Load
    # -------------------------------------------------

    rows = load_dataset()

    print()
    print(
        f"Total clean records: {len(rows)}"
    )

    # -------------------------------------------------
    # Expected dataset size
    # -------------------------------------------------

    if len(rows) != 236:

        raise ValueError(
            f"\nExpected 236 corrected clips, "
            f"but found {len(rows)}."
        )

    # -------------------------------------------------
    # Check duplicates
    # -------------------------------------------------

    check_duplicates(rows)

    print(
        "Duplicate check: PASSED"
    )

    # -------------------------------------------------
    # Shuffle
    # -------------------------------------------------

    random.seed(
        RANDOM_SEED
    )

    random.shuffle(
        rows
    )

    # -------------------------------------------------
    # Calculate split
    # -------------------------------------------------

    train_size = int(
        len(rows) * TRAIN_RATIO
    )

    train_rows = rows[
        :train_size
    ]

    eval_rows = rows[
        train_size:
    ]

    # -------------------------------------------------
    # Check split
    # -------------------------------------------------

    if len(train_rows) + len(eval_rows) != len(rows):

        raise ValueError(
            "Train/evaluation split error."
        )

    # -------------------------------------------------
    # Check overlap
    # -------------------------------------------------

    train_paths = {
        row[0]
        for row in train_rows
    }

    eval_paths = {
        row[0]
        for row in eval_rows
    }

    overlap = (
        train_paths
        &
        eval_paths
    )

    if overlap:

        raise ValueError(
            "ERROR: Audio clips appear "
            "in both train and evaluation sets."
        )

    print(
        "Train/evaluation overlap: NONE"
    )

    # -------------------------------------------------
    # Create output directory
    # -------------------------------------------------

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True
    )

    # -------------------------------------------------
    # Save files
    # -------------------------------------------------

    save_tsv(
        TRAIN_FILE,
        train_rows
    )

    save_tsv(
        EVAL_FILE,
        eval_rows
    )

    # -------------------------------------------------
    # Summary
    # -------------------------------------------------

    print()
    print("-" * 70)

    print(
        f"Training clips    : {len(train_rows)}"
    )

    print(
        f"Evaluation clips  : {len(eval_rows)}"
    )

    print(
        f"Total              : "
        f"{len(train_rows) + len(eval_rows)}"
    )

    print()
    print(
        f"Training file:"
    )

    print(
        TRAIN_FILE
    )

    print()
    print(
        f"Evaluation file:"
    )

    print(
        EVAL_FILE
    )

    print("-" * 70)

    print()
    print(
        "DATASET SPLIT COMPLETED SUCCESSFULLY"
    )

    print("=" * 70)


# =====================================================
# RUN
# =====================================================

if __name__ == "__main__":

    main()