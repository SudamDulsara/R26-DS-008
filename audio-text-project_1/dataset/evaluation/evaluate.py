from jiwer import wer, cer

# =====================================================
# LOAD GROUND TRUTH
# =====================================================

def load_ground_truth(file_path):

    with open(
        file_path,
        "r",
        encoding="utf-8"
    ) as f:

        text = f.read()

    return text.strip()

# =====================================================
# EVALUATE TRANSCRIPT
# =====================================================

def evaluate_transcript(
    predicted_text,
    ground_truth_text
):

    # -------------------------------------------------
    # WER
    # -------------------------------------------------

    wer_score = wer(
        ground_truth_text,
        predicted_text
    )

    # -------------------------------------------------
    # CER
    # -------------------------------------------------

    cer_score = cer(
        ground_truth_text,
        predicted_text
    )

    # -------------------------------------------------
    # ACCURACY
    # -------------------------------------------------

    accuracy = max(
        0,
        (1 - wer_score) * 100
    )

    return {

        "WER": round(
            wer_score * 100,
            2
        ),

        "CER": round(
            cer_score * 100,
            2
        ),

        "Accuracy": round(
            accuracy,
            2
        )
    }