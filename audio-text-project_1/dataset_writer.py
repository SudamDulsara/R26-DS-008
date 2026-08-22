# =====================================================
# dataset_writer.py
# =====================================================

import os

# =====================================================
# PATHS
# =====================================================

DATASET_DIR = "dataset"

TRAIN_FILE = os.path.join(
    DATASET_DIR,
    "train.tsv"
)


# =====================================================
# CREATE DATASET
# =====================================================

def initialize_dataset():

    os.makedirs(
        DATASET_DIR,
        exist_ok=True
    )

    if not os.path.exists(
        TRAIN_FILE
    ):

        with open(
            TRAIN_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            pass


# =====================================================
# WRITE DATASET
# =====================================================

def write_dataset(results):

    initialize_dataset()

    if len(results) == 0:

        print(
            "\nNo transcripts to save."
        )

        return

    with open(

        TRAIN_FILE,

        "a",

        encoding="utf-8"

    ) as f:

        for item in results:

            line = (

                f"{item['audio_path']}\t"

                f"{item['text']}\t"

                f"{item['duration']}"

                "\n"

            )

            f.write(line)

    print()

    print(
        "=" * 60
    )

    print(
        f"Saved {len(results)} records to train.tsv"
    )

    print(
        "=" * 60
    )


# =====================================================
# TEST
# =====================================================

if __name__ == "__main__":

    sample = [

        {

            "audio_path":
                r"G:\My Drive\Sinhala Dataset Clips\chunk_99999.wav",

            "text":
                "මෙය පරීක්ෂණයකි.",

            "duration":
                10.0

        }

    ]

    write_dataset(
        sample
    )