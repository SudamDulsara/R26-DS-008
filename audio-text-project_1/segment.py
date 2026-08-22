# =========================================================
# segment.py
# =========================================================

from pydub import AudioSegment
from pydub.silence import split_on_silence

from silero_vad import (
    load_silero_vad,
    get_speech_timestamps
)

from config import GOOGLE_DRIVE_FOLDER

import librosa
import torch

import os
import re


# =========================================================
# CONFIG
# =========================================================

# Local folder used ONLY for temporary chunks
TEMP_CHUNK_DIR = "dataset/clips"

CHUNK_LENGTH_MS = 10000

MIN_CHUNK_MS = 2500

MIN_SILENCE_LEN = 650

KEEP_SILENCE = 300


# =========================================================
# LOAD SILERO MODEL
# =========================================================

print("Loading Silero VAD...")

vad_model = load_silero_vad()

print("Silero VAD Loaded.")


# =========================================================
# GET NEXT CHUNK INDEX FROM GOOGLE DRIVE
# =========================================================

def get_next_chunk_index():

    # -----------------------------------------------------
    # Check Google Drive folder
    # -----------------------------------------------------

    if not os.path.exists(
        GOOGLE_DRIVE_FOLDER
    ):

        os.makedirs(
            GOOGLE_DRIVE_FOLDER,
            exist_ok=True
        )

        return 0

    # -----------------------------------------------------
    # Get files from Google Drive folder
    # -----------------------------------------------------

    files = os.listdir(
        GOOGLE_DRIVE_FOLDER
    )

    indices = []

    # -----------------------------------------------------
    # Find existing chunk numbers
    # -----------------------------------------------------

    for file in files:

        match = re.search(
            r"chunk_(\d+)\.wav",
            file
        )

        if match:

            indices.append(
                int(
                    match.group(1)
                )
            )

    # -----------------------------------------------------
    # No existing chunks
    # -----------------------------------------------------

    if not indices:

        return 0

    # -----------------------------------------------------
    # Next number after highest Drive chunk
    # -----------------------------------------------------

    return max(indices) + 1


# =========================================================
# CHECK SPEECH USING SILERO
# =========================================================

def contains_speech(audio_path):

    audio, sr = librosa.load(

        audio_path,

        sr=16000,

        mono=True

    )

    audio = torch.from_numpy(
        audio
    ).float()

    speech = get_speech_timestamps(

        audio,

        vad_model,

        sampling_rate=16000,

        threshold=0.5,

        min_speech_duration_ms=250,

        min_silence_duration_ms=150

    )

    return len(speech) > 0


# =========================================================
# VALIDATE CHUNK
# =========================================================

def is_valid_chunk(chunk):

    # -----------------------------------------------------
    # Minimum duration
    # -----------------------------------------------------

    if len(chunk) < MIN_CHUNK_MS:

        return False

    # -----------------------------------------------------
    # Minimum volume
    # -----------------------------------------------------

    if chunk.dBFS < -38:

        return False

    # -----------------------------------------------------
    # Minimum RMS
    # -----------------------------------------------------

    if chunk.rms < 100:

        return False

    return True


# =========================================================
# SPLIT AUDIO
# =========================================================

def split_audio(
    file_path,
    video
):

    # -----------------------------------------------------
    # Create temporary local folder
    # -----------------------------------------------------

    os.makedirs(
        TEMP_CHUNK_DIR,
        exist_ok=True
    )

    # -----------------------------------------------------
    # Load cleaned audio
    # -----------------------------------------------------

    audio = AudioSegment.from_wav(
        file_path
    )

    # -----------------------------------------------------
    # GET NEXT NUMBER FROM GOOGLE DRIVE
    # -----------------------------------------------------

    start_idx = get_next_chunk_index()

    print()
    print(
        "=" * 60
    )

    print(
        f"Google Drive highest existing chunk: "
        f"{start_idx - 1 if start_idx > 0 else 'None'}"
    )

    print(
        f"Next chunk number: {start_idx}"
    )

    print(
        "=" * 60
    )

    chunks = []

    i = 0

    # =====================================================
    # AUDIO INFORMATION
    # =====================================================

    print(
        "\nProcessing Audio..."
    )

    print(
        f"Duration: "
        f"{len(audio) / 1000:.2f} seconds"
    )

    # =====================================================
    # SPLIT ON SILENCE
    # =====================================================

    segments = split_on_silence(

        audio,

        min_silence_len=MIN_SILENCE_LEN,

        silence_thresh=audio.dBFS - 14,

        keep_silence=KEEP_SILENCE

    )

    print(
        f"\nFound {len(segments)} speech segments"
    )

    # =====================================================
    # PROCESS SEGMENTS
    # =====================================================

    for segment in segments:

        # -------------------------------------------------
        # Skip very short segments
        # -------------------------------------------------

        if len(segment) < MIN_CHUNK_MS:

            continue

        # -------------------------------------------------
        # Divide segment into maximum 10-second chunks
        # -------------------------------------------------

        for j in range(

            0,

            len(segment),

            CHUNK_LENGTH_MS

        ):

            chunk = segment[
                j:j + CHUNK_LENGTH_MS
            ]

            # -------------------------------------------------
            # BASIC VALIDATION
            # -------------------------------------------------

            if not is_valid_chunk(
                chunk
            ):

                continue

            # -------------------------------------------------
            # CREATE UNIQUE CHUNK NUMBER
            #
            # Number comes from Google Drive.
            #
            # Example:
            # Drive highest = 310
            # First candidate = 311
            # Next candidate = 312
            # -------------------------------------------------

            chunk_number = (
                start_idx + i
            )

            filename = os.path.join(

                TEMP_CHUNK_DIR,

                f"chunk_{chunk_number:05d}.wav"

            )

            # -------------------------------------------------
            # Increment immediately
            #
            # This prevents a rejected chunk from reusing
            # the same filename.
            # -------------------------------------------------

            i += 1

            # -------------------------------------------------
            # EXPORT TEMPORARY LOCAL CHUNK
            # -------------------------------------------------

            chunk.export(

                filename,

                format="wav",

                codec="pcm_s16le"

            )

            print()
            print(
                f"Checking Speech: {filename}"
            )

            # =================================================
            # SILERO VAD
            # =================================================

            try:

                speech_found = contains_speech(
                    filename
                )

            except Exception as e:

                print(
                    f"Silero Error: {e}"
                )

                # ---------------------------------------------
                # Delete failed temporary chunk
                # ---------------------------------------------

                if os.path.exists(
                    filename
                ):

                    os.remove(
                        filename
                    )

                continue

            # =================================================
            # REJECT IF NO SPEECH
            # =================================================

            if not speech_found:

                print(
                    "Rejected (No Speech)"
                )

                if os.path.exists(
                    filename
                ):

                    os.remove(
                        filename
                    )

                continue

            # =================================================
            # VALID CHUNK
            # =================================================

            chunks.append(
                filename
            )

            print(
                f"Saved temporary chunk: {filename}"
            )

    # =====================================================
    # SUMMARY
    # =====================================================

    print()

    print(
        "=" * 60
    )

    print(
        f"Total Valid Chunks: {len(chunks)}"
    )

    print(
        "=" * 60
    )

    # =====================================================
    # DELETE TEMPORARY CLEAN AUDIO
    # =====================================================

    try:

        if os.path.exists(
            file_path
        ):

            os.remove(
                file_path
            )

            print(
                "\nTemporary clean audio deleted."
            )

    except Exception as e:

        print(
            f"Could not delete temporary "
            f"clean audio: {e}"
        )

    # =====================================================
    # RETURN TEMPORARY CHUNKS
    #
    # These are passed to transcribe_dataset.py.
    #
    # Valid clips will subsequently be:
    #
    # 1. Transcribed
    # 2. Copied to Google Drive
    # 3. Saved in database
    # 4. Deleted locally
    #
    # =====================================================

    return chunks


# =========================================================
# TEST
# =========================================================

if __name__ == "__main__":

    from collector import get_next_video

    video = get_next_video()

    if video:

        chunks = split_audio(

            "temp/audio_clean.wav",

            video

        )

        print()

        print(
            "Segmentation Complete"
        )

        print(
            f"Generated {len(chunks)} temporary chunks."
        )