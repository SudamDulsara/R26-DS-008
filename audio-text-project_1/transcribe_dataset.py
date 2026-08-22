# =====================================================
# transcribe_dataset.py
# =====================================================

from transcribe import transcribe_audio

from database import (
    save_clip,
    update_clip_transcript
)

from drive_sync import copy_to_drive

from config import GOOGLE_DRIVE_FOLDER

from pydub import AudioSegment

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

        print(
            "Skipped (Empty Transcript)"
        )

        return False

    if not is_sinhala(text):

        print(
            "Skipped (Non-Sinhala)"
        )

        return False

    if contains_music_tags(text):

        print(
            "Skipped (Music/Applause)"
        )

        return False

    if is_hallucination(text):

        print(
            "Skipped (Hallucination)"
        )

        return False

    if too_short(text):

        print(
            "Skipped (Too Short)"
        )

        return False

    return True


# =====================================================
# GET ACTUAL CLIP DURATION
# =====================================================

def get_clip_duration(audio_path):

    try:

        audio = AudioSegment.from_wav(
            audio_path
        )

        return round(
            len(audio) / 1000,
            2
        )

    except Exception as e:

        print(
            f"Could not determine clip duration: {e}"
        )

        return 0.0


# =====================================================
# TRANSCRIBE DATASET
# =====================================================

def transcribe_dataset(chunks, video):

    """
    Transcribes ONLY the chunks generated
    from the current video.

    Processing order:

        1. Transcribe local temporary clip
        2. Validate transcript
        3. If invalid:
               delete local clip
               do not copy to Drive
               do not save to database

        4. If valid:
               copy clip to Google Drive
               save metadata to database
               save transcript to database
               add record to dataset
               delete local temporary clip

    Google Drive is the permanent storage.

    dataset/clips is temporary processing storage.
    """

    results = []

    print()
    print(
        "=" * 60
    )

    print(
        "TRANSCRIBING AUDIO"
    )

    print(
        "=" * 60
    )

    total = len(chunks)

    # =====================================================
    # PROCESS EACH CHUNK
    # =====================================================

    for i, clip in enumerate(
        chunks,
        start=1
    ):

        print()

        print(
            f"[{i}/{total}]"
        )

        print(
            f"Audio : {clip}"
        )

        # =================================================
        # CHECK LOCAL FILE
        # =================================================

        if not os.path.exists(clip):

            print(
                "Skipped - local audio file not found."
            )

            continue

        # =================================================
        # GET ACTUAL DURATION
        # =================================================

        duration = get_clip_duration(
            clip
        )

        print(
            f"Duration : {duration} seconds"
        )

        # =================================================
        # TRANSCRIBE
        # =================================================

        try:

            transcript = transcribe_audio(
                clip
            )

        except Exception as e:

            print()
            print(
                f"Transcription failed: {e}"
            )

            print(
                "Keeping local clip for retry."
            )

            continue

        # =================================================
        # VALIDATE TRANSCRIPT
        # =================================================

        if not validate_transcript(
            transcript
        ):

            # ---------------------------------------------
            # INVALID CLIP
            #
            # DO NOT:
            # - copy to Drive
            # - save to database
            # - add to dataset
            # ---------------------------------------------

            if os.path.exists(clip):

                try:

                    os.remove(
                        clip
                    )

                    print(
                        "Deleted invalid local clip."
                    )

                except Exception as e:

                    print(
                        f"Could not delete invalid "
                        f"clip: {e}"
                    )

            continue

        # =================================================
        # VALID TRANSCRIPT
        # =================================================

        print()

        print(
            f"Transcript : {transcript}"
        )

        # =================================================
        # COPY VALID CLIP TO GOOGLE DRIVE
        # =================================================

        print()

        print(
            "Copying valid clip to Google Drive..."
        )

        try:

            drive_success = copy_to_drive(
                clip
            )

        except Exception as e:

            print(
                f"Google Drive copy failed: {e}"
            )

            drive_success = False

        # =================================================
        # DRIVE COPY FAILED
        # =================================================

        if not drive_success:

            print(
                "Google Drive Sync Failed."
            )

            print(
                "Keeping local clip for retry."
            )

            continue

        print(
            "Google Drive Sync Completed"
        )

        # =================================================
        # GOOGLE DRIVE PATH
        # =================================================

        drive_audio_path = os.path.join(

            GOOGLE_DRIVE_FOLDER,

            os.path.basename(clip)

        )

        # =================================================
        # SAVE CLIP TO DATABASE
        # =================================================

        try:

            save_clip(

                video_id=video["video_id"],

                clip_name=os.path.basename(
                    clip
                ),

                duration=duration

            )

            update_clip_transcript(

                clip_name=os.path.basename(
                    clip
                ),

                transcript=transcript

            )

            print(
                "Clip metadata saved to database."
            )

        except Exception as e:

            print()

            print(
                f"Database update failed: {e}"
            )

            print(
                "Local clip will be kept."
            )

            continue

        # =================================================
        # ADD VALID RECORD TO DATASET
        # =================================================

        results.append(

            {

                "audio_path": drive_audio_path,

                "text": transcript,

                "duration": duration

            }

        )

        # =================================================
        # DELETE LOCAL TEMPORARY CLIP
        # =================================================

        if os.path.exists(clip):

            try:

                os.remove(
                    clip
                )

                print(
                    "Local temporary clip deleted."
                )

            except Exception as e:

                print(
                    f"Could not delete local clip: {e}"
                )

    # =====================================================
    # FINISHED
    # =====================================================

    print()

    print(
        "=" * 60
    )

    print(
        f"Finished {len(results)} valid transcripts"
    )

    print(
        "=" * 60
    )

    return results