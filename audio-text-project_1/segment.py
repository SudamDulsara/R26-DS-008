# segment.py

from pydub import AudioSegment
from pydub.silence import split_on_silence

import os
import re


# =========================================================
# CONFIG
# =========================================================

OUTPUT_DIR = "dataset/clips"

CHUNK_LENGTH_MS = 10000      # 10 seconds
MIN_CHUNK_MS = 3000          # minimum useful speech
MIN_SILENCE_LEN = 500        # silence detection
KEEP_SILENCE = 250           # preserve natural pauses


# =========================================================
# GET NEXT CHUNK INDEX
# =========================================================

def get_next_chunk_index(directory=OUTPUT_DIR):

    if not os.path.exists(directory):

        os.makedirs(directory, exist_ok=True)

        return 0

    files = os.listdir(directory)

    indices = [

        int(re.search(r'chunk_(\d+)', f).group(1))

        for f in files

        if re.search(r'chunk_(\d+)', f)
    ]

    return max(indices) + 1 if indices else 0


# =========================================================
# AUDIO QUALITY VALIDATION
# =========================================================

def is_valid_chunk(chunk):

    # -----------------------------------------------------
    # TOO SHORT
    # -----------------------------------------------------

    if len(chunk) < MIN_CHUNK_MS:
        return False

    # -----------------------------------------------------
    # TOO QUIET
    # -----------------------------------------------------

    if chunk.dBFS < -40:
        return False

    # -----------------------------------------------------
    # VERY LOW ENERGY
    # -----------------------------------------------------

    if chunk.rms < 100:
        return False

    return True


# =========================================================
# SPLIT AUDIO
# =========================================================

def split_audio(file_path):

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # -----------------------------------------------------
    # LOAD AUDIO
    # -----------------------------------------------------

    audio = AudioSegment.from_wav(file_path)

    # Whisper preferred format
    audio = audio.set_frame_rate(16000).set_channels(1)

    start_idx = get_next_chunk_index()

    chunks = []

    i = 0

    print("\n🎵 Processing Audio...")
    print(f"📏 Duration: {len(audio)/1000:.2f} seconds")

    # -----------------------------------------------------
    # SILENCE-BASED SPEECH SEGMENTATION
    # -----------------------------------------------------

    segments = split_on_silence(

        audio,

        min_silence_len=MIN_SILENCE_LEN,

        # adaptive threshold
        silence_thresh=audio.dBFS - 16,

        keep_silence=KEEP_SILENCE
    )

    print(f"\n🔍 Found {len(segments)} speech segments")

    # -----------------------------------------------------
    # PROCESS EACH SEGMENT
    # -----------------------------------------------------

    for segment in segments:

        # skip tiny segments
        if len(segment) < MIN_CHUNK_MS:
            continue

        # -------------------------------------------------
        # SPLIT LONG SEGMENTS INTO 10s CHUNKS
        # -------------------------------------------------

        for j in range(0, len(segment), CHUNK_LENGTH_MS):

            chunk = segment[j:j + CHUNK_LENGTH_MS]

            # -------------------------------------------------
            # VALIDATE CHUNK
            # -------------------------------------------------

            if not is_valid_chunk(chunk):

                print("⚠ Invalid chunk skipped")

                continue

            # -------------------------------------------------
            # SAVE CHUNK
            # -------------------------------------------------

            filename = os.path.join(
                OUTPUT_DIR,
                f"chunk_{start_idx + i:05d}.wav"
            )

            chunk.export(
                filename,
                format="wav",
                codec="pcm_s16le"
            )

            chunks.append(filename)

            print(f"✅ Saved: {filename}")

            i += 1

    print(f"\n🎵 Total Valid Chunks: {len(chunks)}")

    return chunks


# =========================================================
# CLI TESTING
# =========================================================

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to WAV audio"
    )

    args = parser.parse_args()

    print("\n✂️ Segmenting Audio...")
    print("-" * 40)

    chunks = split_audio(args.input)

    print("\n✅ Segmentation Complete")