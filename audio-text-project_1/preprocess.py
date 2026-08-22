# =====================================================
# preprocess.py
# =====================================================

import os
import subprocess

from pydub import AudioSegment

# =====================================================
# OUTPUT FILE
# =====================================================

OUTPUT_FILE = "temp/audio_clean.wav"

# =====================================================
# PREPROCESS AUDIO
# =====================================================

def preprocess_audio(input_path):

    """
    Preprocess audio before segmentation.

    Steps:

    1. Convert to mono
    2. Convert to 16 kHz
    3. Apply high-pass filter
    4. Apply low-pass filter
    5. Normalize volume
    """

    if not os.path.exists(input_path):

        raise FileNotFoundError(

            f"{input_path} not found."

        )

    os.makedirs(

        "temp",

        exist_ok=True

    )

    # -------------------------------------------------
    # FFMPEG FILTERING
    # -------------------------------------------------

    command = [

        "ffmpeg",

        "-y",

        "-i",
        input_path,

        "-af",

        "highpass=f=100,lowpass=f=7000",

        "-ac",
        "1",

        "-ar",
        "16000",

        OUTPUT_FILE

    ]

    subprocess.run(

        command,

        stdout=subprocess.DEVNULL,

        stderr=subprocess.DEVNULL,

        check=True

    )

    # -------------------------------------------------
    # NORMALIZE AUDIO
    # -------------------------------------------------

    audio = AudioSegment.from_wav(

        OUTPUT_FILE

    )

    audio = audio.normalize()

    audio.export(

        OUTPUT_FILE,

        format="wav"

    )

    print("\nAudio preprocessing completed.")

    return OUTPUT_FILE


# =====================================================
# TEST
# =====================================================

if __name__ == "__main__":

    cleaned = preprocess_audio(

        "temp/audio.wav"

    )

    print()

    print("Saved Clean Audio:")

    print(cleaned)