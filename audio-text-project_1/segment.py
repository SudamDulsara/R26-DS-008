# =========================================================
# segment.py
# =========================================================

from pydub import AudioSegment
from pydub.silence import split_on_silence

from silero_vad import (
    load_silero_vad,
    get_speech_timestamps
)

from database import save_clip

import librosa
import torch

import os
import re

# =========================================================
# CONFIG
# =========================================================

OUTPUT_DIR = "dataset/clips"

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
# GET NEXT CHUNK INDEX
# =========================================================

def get_next_chunk_index(directory=OUTPUT_DIR):

    if not os.path.exists(directory):

        os.makedirs(directory, exist_ok=True)

        return 0

    files = os.listdir(directory)

    indices = [

        int(
            re.search(
                r"chunk_(\d+)",
                file
            ).group(1)
        )

        for file in files

        if re.search(
            r"chunk_(\d+)",
            file
        )

    ]

    return max(indices) + 1 if indices else 0


# =========================================================
# CHECK SPEECH USING SILERO
# =========================================================

def contains_speech(audio_path):

    audio, sr = librosa.load(

        audio_path,

        sr=16000,

        mono=True

    )

    audio = torch.from_numpy(audio).float()

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

    if len(chunk) < MIN_CHUNK_MS:

        return False

    if chunk.dBFS < -38:

        return False

    if chunk.rms < 100:

        return False

    return True


# =========================================================
# SPLIT AUDIO
# =========================================================

def split_audio(file_path, video):

    os.makedirs(

        OUTPUT_DIR,

        exist_ok=True

    )

    audio = AudioSegment.from_wav(file_path)

    start_idx = get_next_chunk_index()

    chunks = []

    i = 0

    print("\nProcessing Audio...")

    print(
        f"Duration: {len(audio)/1000:.2f} seconds"
    )

    segments = split_on_silence(

        audio,

        min_silence_len=MIN_SILENCE_LEN,

        silence_thresh=audio.dBFS - 14,

        keep_silence=KEEP_SILENCE

    )

    print(
        f"\nFound {len(segments)} speech segments"
    )

    for segment in segments:

        if len(segment) < MIN_CHUNK_MS:

            continue

        for j in range(

            0,

            len(segment),

            CHUNK_LENGTH_MS

        ):

            chunk = segment[
                j:j + CHUNK_LENGTH_MS
            ]

            if not is_valid_chunk(chunk):

                continue

            filename = os.path.join(

                OUTPUT_DIR,

                f"chunk_{start_idx+i:05d}.wav"

            )

            chunk.export(

                filename,

                format="wav",

                codec="pcm_s16le"

            )

            print(f"Checking Speech: {filename}")

            try:

                if not contains_speech(filename):

                    print("Rejected (No Speech)")

                    os.remove(filename)

                    continue

            except Exception as e:

                print(f"Silero Error: {e}")

                if os.path.exists(filename):

                    os.remove(filename)

                continue

            # -------------------------------------------------
            # SAVE CLIP METADATA TO DATABASE
            # -------------------------------------------------

            save_clip(

                video_id=video["video_id"],

                clip_name=os.path.basename(filename),

                duration=round(len(chunk) / 1000, 2)

            )

            chunks.append(filename)

            print(f"Saved: {filename}")

            i += 1

    print(

        f"\nTotal Valid Chunks: {len(chunks)}"

    )

    try:

        if os.path.exists(file_path):

            os.remove(file_path)

            print(

                "\nTemporary clean audio deleted."

            )

    except Exception as e:

        print(

            f"Could not delete temporary file: {e}"

        )

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

        print("Segmentation Complete")

        print(

            f"Generated {len(chunks)} chunks."

        )