# =====================================================
# transcribe_dataset.py
# =====================================================

from transcribe import transcribe_audio

from database import (
    save_clip,
    update_clip_transcript,
    update_clip_drive_file_id
)

from drive_sync import copy_to_drive

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
               do not upload
               do not save to database

        4. If valid:
               upload clip to Google Drive
               obtain Google Drive file ID
               save clip metadata
               save transcript
               save Drive file ID
               delete local temporary clip

    Google Drive is the permanent audio storage.

    Local audio files are temporary processing files.

    SQLite database stores:

        - video_id
        - clip_name
        - duration
        - transcript
        - drive_file_id

    No TSV file is generated.
    """

    successful_clips = []

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
        # UPLOAD TO GOOGLE DRIVE
        # =================================================

        print()

        print(
            "Uploading valid clip to Google Drive..."
        )

        try:

            drive_file_id = copy_to_drive(
                clip
            )

        except Exception as e:

            print(
                f"Google Drive upload failed: {e}"
            )

            drive_file_id = None

        # =================================================
        # DRIVE UPLOAD FAILED
        # =================================================

        if not drive_file_id:

            print(
                "Google Drive upload failed."
            )

            print(
                "Keeping local clip for retry."
            )

            continue

        print(
            "Google Drive upload completed."
        )

        print(
            f"Drive File ID: {drive_file_id}"
        )

        # =================================================
        # SAVE TO DATABASE
        # =================================================

        try:

            clip_name = os.path.basename(
                clip
            )

            # ---------------------------------------------
            # SAVE CLIP METADATA
            # ---------------------------------------------

            save_clip(

                video_id=video["video_id"],

                clip_name=clip_name,

                duration=duration

            )

            # ---------------------------------------------
            # SAVE TRANSCRIPT
            # ---------------------------------------------

            update_clip_transcript(

                clip_name=clip_name,

                transcript=transcript

            )

            # ---------------------------------------------
            # SAVE GOOGLE DRIVE FILE ID
            # ---------------------------------------------

            update_clip_drive_file_id(

                clip_name=clip_name,

                drive_file_id=drive_file_id

            )

            print(
                "Clip metadata saved to database."
            )

            print(
                f"Drive File ID saved: {drive_file_id}"
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
        # RECORD SUCCESS
        # =================================================

        successful_clips.append(
            {
                "clip_name": clip_name,
                "transcript": transcript,
                "duration": duration,
                "drive_file_id": drive_file_id
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
        f"Finished {len(successful_clips)} valid transcripts"
    )

    print(
        "=" * 60
    )

    return successful_clips