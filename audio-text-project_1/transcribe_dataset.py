# =====================================================
# transcribe_dataset.py
# =====================================================

from transcribe import transcribe_audio

from database import (
    save_clip,
)

from topic_classifier import (
    classify_topic_from_video
)

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

    ratio = (
        sinhala_chars /
        total_chars
    )

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

    characters = text.replace(
        " ",
        ""
    )

    unique = set(
        characters
    )

    if len(unique) == 1:
        return True

    words = text.split()

    if len(words) >= 4:

        counts = Counter(
            words
        )

        most_common = (
            counts
            .most_common(1)[0][1]
        )

        if most_common >= (
            len(words) * 0.8
        ):
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
# READ AUDIO BYTES
# =====================================================

def read_audio_bytes(audio_path):

    with open(
        audio_path,
        "rb"
    ) as audio_file:

        return audio_file.read()


# =====================================================
# FORMAT WORD TIMESTAMPS
# =====================================================

def format_word_timestamps(
    word_timestamps
):

    lines = []

    for item in word_timestamps:

        if not isinstance(
            item,
            dict
        ):
            continue

        word = str(
            item.get(
                "word",
                ""
            )
        ).strip()

        start = item.get(
            "start"
        )

        end = item.get(
            "end"
        )

        if not word:
            continue

        if start is None or end is None:
            continue

        try:

            start = float(
                start
            )

            end = float(
                end
            )

        except (
            TypeError,
            ValueError
        ):

            continue

        lines.append(
            f"{word} - {start:.2f}s-{end:.2f}s"
        )

    return "\n".join(
        lines
    )


# =====================================================
# TRANSCRIBE DATASET
# =====================================================

def transcribe_dataset(
    chunks,
    video
):

    """
    Transcribes only the chunks generated
    from the current video.

    Processing order:

        1. Transcribe local temporary clip
        2. Obtain word-level timestamps
        3. Validate transcript
        4. Classify topic
        5. Read audio bytes
        6. Format word timestamps
        7. Save audio + transcript +
           timestamps + topic + duration to SQLite
        8. Delete local temporary clip

    SQLite is the permanent storage location
    for the generated audio clips and metadata.

    No Google Drive is used.
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

    total = len(
        chunks
    )

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

        if not os.path.exists(
            clip
        ):

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
        # TRANSCRIBE + WORD TIMESTAMPS
        # =================================================

        try:

            transcription_result = (
                transcribe_audio(
                    clip
                )
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

        if not transcription_result:

            print(
                "Transcription returned no result."
            )

            print(
                "Keeping local clip for retry."
            )

            continue

        transcript = (
            transcription_result.get(
                "text",
                ""
            )
        )

        word_timestamps = (
            transcription_result.get(
                "word_timestamps",
                []
            )
        )

        # =================================================
        # VALIDATE TRANSCRIPT
        # =================================================

        if not validate_transcript(
            transcript
        ):

            if os.path.exists(
                clip
            ):

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
        # REQUIRE WORD TIMESTAMPS
        # =================================================

        if not word_timestamps:

            print(
                "Skipped - no word-level timestamps."
            )

            if os.path.exists(
                clip
            ):

                try:

                    os.remove(
                        clip
                    )

                except Exception:
                    pass

            continue

        # =================================================
        # VALID TRANSCRIPT
        # =================================================

        print()

        print(
            f"Transcript : {transcript}"
        )

        # =================================================
        # FORMAT WORD TIMESTAMPS
        # =================================================

        formatted_word_timestamps = (
            format_word_timestamps(
                word_timestamps
            )
        )

        print()

        print(
            "Word timestamps:"
        )

        print(
            formatted_word_timestamps
        )

        # =================================================
        # TOPIC CLASSIFICATION
        # =================================================

        topic = (
            classify_topic_from_video(
                video,
                transcript=transcript
            )
        )

        print()

        print(
            f"Topic : {topic}"
        )

        # =================================================
        # READ AUDIO
        # =================================================

        try:

            audio_bytes = (
                read_audio_bytes(
                    clip
                )
            )

        except Exception as e:

            print(
                f"Could not read audio: {e}"
            )

            print(
                "Keeping local clip for retry."
            )

            continue

        # =================================================
        # CLIP NAME
        # =================================================

        clip_name = os.path.splitext(
            os.path.basename(
                clip
            )
        )[0]

        # =================================================
        # SAVE EVERYTHING TO DATABASE
        # =================================================

        try:

            save_clip(

                clip_id=clip_name,

                video_id=video[
                    "video_id"
                ],

                audio=audio_bytes,

                topic=topic,

                transcription=transcript,

                word_timestamps=(
                    formatted_word_timestamps
                ),

                duration=duration

            )

            print(
                "Audio + transcript + "
                "timestamps + topic + duration "
                "saved to database."
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
                "topic": topic,
                "duration": duration,
                "word_timestamps": (
                    word_timestamps
                )
            }
        )

        # =================================================
        # DELETE LOCAL TEMPORARY CLIP
        # =================================================

        if os.path.exists(
            clip
        ):

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