# =========================================================
# segment.py
# =========================================================

from pydub import AudioSegment
from pydub.silence import split_on_silence

from silero_vad import (
    load_silero_vad,
    get_speech_timestamps
)

from config import (
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_DRIVE_TOKEN
)

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

import librosa
import torch

import os
import re


# =========================================================
# CONFIG
# =========================================================

# Temporary local folder.
#
# Audio files are only stored here while being processed.
# They are uploaded to Google Drive after successful
# transcription and then deleted.
#
TEMP_CHUNK_DIR = "dataset/clips"


# Maximum chunk duration
CHUNK_LENGTH_MS = 10000


# Minimum chunk duration
MIN_CHUNK_MS = 2500


# Silence detection
MIN_SILENCE_LEN = 650

KEEP_SILENCE = 300


# =========================================================
# GOOGLE DRIVE SETTINGS
# =========================================================

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive"
]


# =========================================================
# LOAD SILERO MODEL
# =========================================================

print(
    "Loading Silero VAD..."
)

vad_model = load_silero_vad()

print(
    "Silero VAD Loaded."
)


# =========================================================
# GET GOOGLE DRIVE SERVICE
# =========================================================

def get_drive_service():

    if not os.path.exists(
        GOOGLE_DRIVE_TOKEN
    ):

        print(
            "❌ Google Drive token not found."
        )

        return None

    try:

        credentials = (
            Credentials.from_authorized_user_file(

                GOOGLE_DRIVE_TOKEN,

                DRIVE_SCOPES

            )
        )

        service = build(

            "drive",

            "v3",

            credentials=credentials

        )

        return service

    except Exception as e:

        print(
            f"❌ Google Drive connection failed: {e}"
        )

        return None


# =========================================================
# GET NEXT CHUNK INDEX FROM GOOGLE DRIVE
# =========================================================

def get_next_chunk_index():

    print(
        "\nChecking Google Drive for existing chunks..."
    )

    service = get_drive_service()

    if service is None:

        print(
            "❌ Could not connect to Google Drive."
        )

        # IMPORTANT:
        # Do NOT start from zero because that can create
        # duplicate chunk names.
        #
        # Returning None allows the caller to stop safely.

        return None

    # -----------------------------------------------------
    # Query files inside the target Google Drive folder
    # -----------------------------------------------------

    query = (

        f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents "

        "and trashed = false"

    )

    try:

        files = []

        page_token = None

        # -------------------------------------------------
        # Retrieve ALL files.
        #
        # We do not assume there are fewer than 1000 files.
        # -------------------------------------------------

        while True:

            response = service.files().list(

                q=query,

                spaces="drive",

                fields="nextPageToken,files(id,name)",

                pageSize=1000,

                pageToken=page_token

            ).execute()

            files.extend(
                response.get(
                    "files",
                    []
                )
            )

            page_token = response.get(
                "nextPageToken"
            )

            if not page_token:

                break

    except Exception as e:

        print(
            f"❌ Could not read Google Drive folder: {e}"
        )

        return None

    # -----------------------------------------------------
    # Find existing chunk numbers
    # -----------------------------------------------------

    indices = []

    for file in files:

        filename = file.get(
            "name",
            ""
        )

        match = re.match(

            r"^chunk_(\d+)\.wav$",

            filename

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

        print(
            "No existing chunk files found in Google Drive."
        )

        print(
            "Starting chunk numbering from 1."
        )

        return 1

    # -----------------------------------------------------
    # Highest existing chunk
    # -----------------------------------------------------

    highest = max(
        indices
    )

    next_index = highest + 1

    print(
        f"Found {len(indices)} existing chunk files."
    )

    print(
        f"Highest existing chunk number: {highest}"
    )

    print(
        f"Next chunk number: {next_index}"
    )

    return next_index


# =========================================================
# CHECK SPEECH USING SILERO
# =========================================================

def contains_speech(
    audio_path
):

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

    return len(
        speech
    ) > 0


# =========================================================
# VALIDATE CHUNK
# =========================================================

def is_valid_chunk(
    chunk
):

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
    # Get next number from Google Drive
    # -----------------------------------------------------

    start_idx = get_next_chunk_index()

    # -----------------------------------------------------
    # SAFETY CHECK
    # -----------------------------------------------------

    if start_idx is None:

        raise RuntimeError(

            "Could not determine the next chunk number "
            "from Google Drive. Pipeline stopped to "
            "prevent duplicate chunk names."

        )

    print()

    print(
        "=" * 60
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
            # -------------------------------------------------

            chunk_number = (

                start_idx + i

            )

            filename = os.path.join(

                TEMP_CHUNK_DIR,

                f"chunk_{chunk_number:05d}.wav"

            )

            # -------------------------------------------------
            # Increment number
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

            f"Generated "
            f"{len(chunks)} temporary chunks."

        )