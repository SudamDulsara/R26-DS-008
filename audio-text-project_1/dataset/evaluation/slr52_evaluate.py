import os

from transcribe import transcribe_audio

from dataset.evaluation.evaluate import (
    evaluate_transcript
)

# =====================================================
# PATHS
# =====================================================

REFERENCE_FILE = (
    "dataset/SLR52/reference.txt"
)

AUDIO_DIR = (
    "dataset/SLR52/audio"
)

# =====================================================
# EVALUATE SLR52
# =====================================================

def evaluate_slr52(sample_limit=10):

    # -------------------------------------------------
    # LOAD REFERENCES
    # -------------------------------------------------

    with open(

        REFERENCE_FILE,

        "r",

        encoding="utf-8"

    ) as f:

        references = [

            line.strip()

            for line in f.readlines()

            if line.strip()
        ]

    # -------------------------------------------------
    # GET AUDIO FILES
    # -------------------------------------------------

    audio_files = sorted([

        file

        for file in os.listdir(AUDIO_DIR)

        if file.endswith(".flac")
    ])

    # -------------------------------------------------
    # CHECK
    # -------------------------------------------------

    if len(audio_files) == 0:

        raise Exception(
            "No audio files found"
        )

    if len(references) == 0:

        raise Exception(
            "No reference transcripts found"
        )

    # -------------------------------------------------
    # LIMIT
    # -------------------------------------------------

    total_samples = min(

        sample_limit,

        len(audio_files),

        len(references)
    )

    print("\nTOTAL AUDIO FILES:")
    print(len(audio_files))

    print("\nTOTAL REFERENCES:")
    print(len(references))

    print("\nTOTAL EVALUATED:")
    print(total_samples)

    # -------------------------------------------------
    # STORAGE
    # -------------------------------------------------

    all_predictions = []

    all_references = []

    results = []

    # =================================================
    # PROCESS
    # =================================================

    for i in range(total_samples):

        audio_file = audio_files[i]

        reference = references[i]

        audio_path = os.path.join(

            AUDIO_DIR,

            audio_file
        )

        print("\n" + "=" * 60)

        print(
            f"PROCESSING: "
            f"{audio_file}"
        )

        # -------------------------------------------------
        # TRANSCRIBE
        # -------------------------------------------------

        predicted = transcribe_audio(
            audio_path
        )

        # -------------------------------------------------
        # HANDLE EMPTY
        # -------------------------------------------------

        if predicted is None:

            predicted = ""

        predicted = predicted.strip()

        # -------------------------------------------------
        # DEBUG OUTPUT
        # -------------------------------------------------

        print("\nPREDICTED:")
        print(predicted)

        print("\nREFERENCE:")
        print(reference)

        # -------------------------------------------------
        # STORE
        # -------------------------------------------------

        all_predictions.append(
            predicted
        )

        all_references.append(
            reference
        )

        # -------------------------------------------------
        # INDIVIDUAL METRICS
        # -------------------------------------------------

        single_metrics = evaluate_transcript(

            predicted,

            reference
        )

        # -------------------------------------------------
        # SAVE RESULT
        # -------------------------------------------------

        results.append({

            "audio": audio_file,

            "prediction": predicted,

            "reference": reference,

            "WER": single_metrics["WER"],

            "CER": single_metrics["CER"],

            "Accuracy": single_metrics[
                "Accuracy"
            ]
        })

    # =================================================
    # FINAL EVALUATION
    # =================================================

    predicted_text = " ".join(
        all_predictions
    )

    reference_text = " ".join(
        all_references
    )

    metrics = evaluate_transcript(

        predicted_text,

        reference_text
    )

    print("\n" + "=" * 60)

    print("FINAL RESULTS")

    print("=" * 60)

    print(
        f"WER: {metrics['WER']}%"
    )

    print(
        f"CER: {metrics['CER']}%"
    )

    print(
        f"Accuracy: "
        f"{metrics['Accuracy']}%"
    )

    print("=" * 60)

    # =================================================
    # RETURN
    # =================================================

    return {

        "metrics": metrics,

        "results": results
    }

# =====================================================
# CLI TEST
# =====================================================

if __name__ == "__main__":

    evaluation = evaluate_slr52(
        sample_limit=10
    )

    print("\nEvaluation Complete")