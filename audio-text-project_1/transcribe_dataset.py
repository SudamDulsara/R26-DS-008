# =====================================================
# transcribe_dataset.py
# =====================================================

from transcribe import transcribe_audio
from database import update_clip_transcript
from drive_sync import copy_to_drive

import os
import re

from collections import Counter

# =====================================================
# CHECK IF TRANSCRIPT IS SINHALA
# =====================================================

def is_sinhala(text):

    if text.strip() == "":

        return False

    sinhala_chars = len(

        re.findall(

            r'[\u0D80-\u0DFF]',

            text

        )

    )

    total_chars = len(

        re.findall(

            r'\S',

            text

        )

    )

    if total_chars == 0:

        return False

    ratio = sinhala_chars / total_chars

    return ratio >= 0.60


# =====================================================
# CHECK FOR MUSIC / APPLAUSE
# =====================================================

def contains_music_tags(text):

    text = text.lower()

    tags = [

        "music",

        "applause",

        "laughter",

        "laugh",

        "clapping",

        "audience",

        "♪",

        "♫",

        "[music]",

        "[applause]",

        "[laughter]"

    ]

    return any(

        tag in text

        for tag in tags

    )


# =====================================================
# HALLUCINATION DETECTION
# =====================================================

def is_hallucination(text):

    text = text.strip()

    if len(text) == 0:

        return True

    # ---------------------------------------------
    # SAME CHARACTER REPEATED
    # ---------------------------------------------

    characters = text.replace(" ", "")

    unique = set(characters)

    if len(unique) == 1:

        return True

    # ---------------------------------------------
    # SAME WORD REPEATED
    # ---------------------------------------------

    words = text.split()

    if len(words) >= 4:

        counts = Counter(words)

        most_common = counts.most_common(1)[0][1]

        if most_common >= len(words) * 0.8:

            return True

    return False


# =====================================================
# TOO SHORT
# =====================================================

def too_short(text):

    words = text.split()

    return len(words) < 2


# =====================================================
# FINAL VALIDATION
# =====================================================

def validate_transcript(text):

    if text.strip() == "":

        print("Skipped (Empty Transcript)")

        return False

    if not is_sinhala(text):

        print("Skipped (Non-Sinhala)")

        return False

    if contains_music_tags(text):

        print("Skipped (Music/Applause)")

        return False

    if is_hallucination(text):

        print("Skipped (Hallucination)")

        return False

    if too_short(text):

        print("Skipped (Too Short)")

        return False

    return True


# =====================================================
# TRANSCRIBE DATASET
# =====================================================

def transcribe_dataset(chunks):

    """
    Transcribes ONLY the chunks that were
    generated from the current video.
    """

    results = []

    print()
    print("=" * 60)
    print("TRANSCRIBING AUDIO")
    print("=" * 60)

    total = len(chunks)

    for i, clip in enumerate(chunks, start=1):

        print()

        print(f"[{i}/{total}]")

        print(f"Audio : {clip}")

        transcript = transcribe_audio(

            clip

        )

        # -----------------------------------------
        # VALIDATE TRANSCRIPT
        # -----------------------------------------

        if not validate_transcript(

            transcript

        ):

            continue

        print(

            f"Transcript : {transcript}"

        )

        # -----------------------------------------
        # UPDATE DATABASE
        # -----------------------------------------

        update_clip_transcript(

            clip_name=os.path.basename(clip),

            transcript=transcript

        )

        # -----------------------------------------
        # COPY TO GOOGLE DRIVE
        # -----------------------------------------

        if copy_to_drive(clip):

            print("Google Drive Sync Completed")

        else:

            print("Google Drive Sync Failed")

        # -----------------------------------------
        # ADD TO DATASET
        # -----------------------------------------

        results.append(

            {

                "audio_path": clip,

                "text": transcript,

                "duration": 10.0

            }

        )

    # -------------------------------------------------
    # FINISHED
    # -------------------------------------------------

    print()

    print("=" * 60)

    print(

        f"Finished {len(results)} transcripts"

    )

    print("=" * 60)

    return results