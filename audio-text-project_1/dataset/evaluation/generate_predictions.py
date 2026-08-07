import os
import csv
import sys

# =========================================================
# FIX LARGE TSV FIELD ERROR
# =========================================================

csv.field_size_limit(10000000)

# =========================================================
# ADD PROJECT ROOT TO PYTHON PATH
# =========================================================

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)

from transcribe import transcribe_audio


# =========================================================
# PATHS
# =========================================================

TRANSCRIPT_FILE = "../SLR52/utt_spk_text.tsv"

AUDIO_FOLDER = "../SLR52/audio"

REFERENCE_OUTPUT = "references.txt"

HYPOTHESIS_OUTPUT = "hypotheses.txt"


# =========================================================
# LOAD SLR52 TRANSCRIPTS
# =========================================================

samples = []

with open(TRANSCRIPT_FILE, "r", encoding="utf-8") as f:

    reader = csv.reader(f, delimiter="\t")

    for row in reader:

        # Skip broken rows
        if len(row) < 3:
            continue

        # -------------------------------------------------
        # SLR52 FORMAT
        # row[0] = utterance id
        # row[1] = speaker id
        # row[2] = transcript
        # -------------------------------------------------

        utt_id = row[0].strip()

        transcript = row[2].strip()

        # Build FLAC path
        audio_path = os.path.join(
            AUDIO_FOLDER,
            f"{utt_id}.flac"
        )

        # Only keep existing files
        if os.path.exists(audio_path):

            samples.append(
                (audio_path, transcript)
            )


print(f"\n✅ Loaded {len(samples)} samples")


# =========================================================
# GENERATE PREDICTIONS
# =========================================================

references = []

hypotheses = []

# ---------------------------------------------------------
# ONLY PROCESS FIRST 20 FOR PP1
# ---------------------------------------------------------

for i, (audio_path, reference) in enumerate(samples[:15]):

    print("\n" + "=" * 60)

    print(f"[{i+1}/20] Processing")

    print(audio_path)

    # -----------------------------------------------------
    # TRANSCRIBE USING YOUR PIPELINE
    # -----------------------------------------------------

    prediction = transcribe_audio(audio_path)

    print("\nREF :")
    print(reference)

    print("\nHYP :")
    print(prediction)

    # -----------------------------------------------------
    # STORE RESULTS
    # -----------------------------------------------------

    references.append(reference)

    hypotheses.append(prediction)


# =========================================================
# SAVE REFERENCES
# =========================================================

with open(REFERENCE_OUTPUT, "w", encoding="utf-8") as f:

    for line in references:

        f.write(line + "\n")


# =========================================================
# SAVE HYPOTHESES
# =========================================================

with open(HYPOTHESIS_OUTPUT, "w", encoding="utf-8") as f:

    for line in hypotheses:

        f.write(line + "\n")


print("\n✅ Prediction generation complete")

print(f"📄 Saved: {REFERENCE_OUTPUT}")

print(f"📄 Saved: {HYPOTHESIS_OUTPUT}")