# =====================================================
# database.py
# =====================================================

import os
import re
import json
import sqlite3

from config import DATABASE_NAME


# =====================================================
# DATABASE CONNECTION
# =====================================================

def get_connection():
    return sqlite3.connect(DATABASE_NAME)


# =====================================================
# TABLE HELPERS
# =====================================================

def _table_exists(cursor, table_name):

    cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table_name,)
    )

    return cursor.fetchone() is not None


def _get_columns(cursor, table_name):

    cursor.execute(
        f'PRAGMA table_info("{table_name}")'
    )

    return [
        row[1]
        for row in cursor.fetchall()
    ]


# =====================================================
# FORMAT WORD TIMESTAMPS
# =====================================================

def _format_word_timestamps(word_timestamps):

    if not word_timestamps:
        return word_timestamps

    # Already formatted text
    if isinstance(word_timestamps, str):

        stripped = word_timestamps.strip()

        if not stripped:
            return stripped

        # Try to detect old JSON format
        try:

            parsed = json.loads(stripped)

            if isinstance(parsed, list):
                word_timestamps = parsed

            else:
                return word_timestamps

        except (json.JSONDecodeError, TypeError):

            # Already normal text format
            return word_timestamps

    if not isinstance(word_timestamps, list):
        return str(word_timestamps)

    lines = []

    for item in word_timestamps:

        if not isinstance(item, dict):
            continue

        word = str(
            item.get("word", "")
        ).strip()

        start = item.get("start")
        end = item.get("end")

        if not word:
            continue

        if start is None or end is None:
            continue

        try:

            start = float(start)
            end = float(end)

        except (TypeError, ValueError):

            continue

        lines.append(
            f"{word} - {start:.2f}s-{end:.2f}s"
        )

    if not lines:
        return ""

    return "\n".join(lines)


# =====================================================
# CREATE DATABASE / SAFE MIGRATION
# =====================================================

def create_database():

    os.makedirs(
        os.path.dirname(
            os.path.abspath(DATABASE_NAME)
        ),
        exist_ok=True
    )

    conn = get_connection()
    cursor = conn.cursor()

    try:

        cursor.execute(
            "PRAGMA foreign_keys=OFF"
        )

        # -------------------------------------------------
        # VIDEOS TABLE
        # -------------------------------------------------

        if not _table_exists(
            cursor,
            "videos"
        ):

            cursor.execute(
                """
                CREATE TABLE videos(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT UNIQUE,
                    title TEXT,
                    url TEXT,
                    duration INTEGER,
                    downloaded INTEGER DEFAULT 0,
                    processed INTEGER DEFAULT 0,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

        else:

            video_columns = _get_columns(
                cursor,
                "videos"
            )

            if "drive_file_id" in video_columns:

                _migrate_videos_without_drive(
                    cursor
                )

        # -------------------------------------------------
        # CLIPS TABLE
        # -------------------------------------------------

        target_clip_columns = [
            "clip_id",
            "video_id",
            "audio",
            "topic",
            "transcription",
            "word_timestamps",
            "duration",
            "verified",
            "language",
            "created_at"
        ]

        if not _table_exists(
            cursor,
            "clips"
        ):

            cursor.execute(
                """
                CREATE TABLE clips(
                    clip_id TEXT PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    audio BLOB,
                    topic TEXT,
                    transcription TEXT,
                    word_timestamps TEXT,
                    duration REAL,
                    verified INTEGER DEFAULT 0,
                    language TEXT DEFAULT 'si',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(video_id)
                    REFERENCES videos(video_id)
                )
                """
            )

        else:

            clip_columns = _get_columns(
                cursor,
                "clips"
            )

            if clip_columns != target_clip_columns:

                print(
                    "\nUpdating clips table structure..."
                )

                _migrate_clips_to_current_schema(
                    cursor,
                    clip_columns
                )

                print(
                    "Clips table updated successfully."
                )

        conn.commit()

    finally:

        cursor.execute(
            "PRAGMA foreign_keys=ON"
        )

        conn.close()


# =====================================================
# MIGRATE VIDEOS
# REMOVE GOOGLE DRIVE COLUMN
# =====================================================

def _migrate_videos_without_drive(cursor):

    cursor.execute(
        """
        ALTER TABLE videos
        RENAME TO videos_old
        """
    )

    cursor.execute(
        """
        CREATE TABLE videos(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT UNIQUE,
            title TEXT,
            url TEXT,
            duration INTEGER,
            downloaded INTEGER DEFAULT 0,
            processed INTEGER DEFAULT 0,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        INSERT INTO videos(
            id,
            video_id,
            title,
            url,
            duration,
            downloaded,
            processed,
            processed_at
        )
        SELECT
            id,
            video_id,
            title,
            url,
            duration,
            downloaded,
            processed,
            processed_at
        FROM videos_old
        """
    )

    cursor.execute(
        "DROP TABLE videos_old"
    )


# =====================================================
# MIGRATE CLIPS
#
# REMOVE:
#   start_time
#   end_time
#
# KEEP:
#   duration
#
# ALSO CONVERT OLD JSON WORD TIMESTAMPS
# TO READABLE TEXT FORMAT
# =====================================================

def _migrate_clips_to_current_schema(
    cursor,
    old_columns
):

    # -------------------------------------------------
    # Rename old table
    # -------------------------------------------------

    cursor.execute(
        """
        ALTER TABLE clips
        RENAME TO clips_old
        """
    )

    # -------------------------------------------------
    # Create new table
    # -------------------------------------------------

    cursor.execute(
        """
        CREATE TABLE clips(
            clip_id TEXT PRIMARY KEY,
            video_id TEXT NOT NULL,
            audio BLOB,
            topic TEXT,
            transcription TEXT,
            word_timestamps TEXT,
            duration REAL,
            verified INTEGER DEFAULT 0,
            language TEXT DEFAULT 'si',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(video_id)
            REFERENCES videos(video_id)
        )
        """
    )

    # -------------------------------------------------
    # Read old records
    # -------------------------------------------------

    cursor.execute(
        """
        SELECT *
        FROM clips_old
        ORDER BY rowid
        """
    )

    rows = cursor.fetchall()

    old_index = {
        name: index
        for index, name in enumerate(old_columns)
    }

    used_ids = set()
    next_fallback = 1

    # -------------------------------------------------
    # Get value from old row
    # -------------------------------------------------

    def value(
        row,
        name,
        default=None
    ):

        index = old_index.get(name)

        if index is None:
            return default

        return row[index]

    # -------------------------------------------------
    # Choose clip ID
    # -------------------------------------------------

    def choose_clip_id(row):

        nonlocal next_fallback

        candidate = value(
            row,
            "clip_id"
        )

        # Old schema may have used clip_name
        if "clip_name" in old_index:

            old_name = value(
                row,
                "clip_name"
            )

            if old_name:
                candidate = old_name

        if candidate is not None:

            candidate = str(
                candidate
            ).strip()

        if not candidate:
            candidate = None

        # Convert numeric IDs
        # e.g. 1 -> chunk_00001

        if candidate and candidate.isdigit():

            candidate = (
                f"chunk_{int(candidate):05d}"
            )

        if (
            candidate
            and candidate not in used_ids
        ):

            used_ids.add(candidate)

            return candidate

        # Fallback
        while True:

            fallback = (
                f"chunk_{next_fallback:05d}"
            )

            next_fallback += 1

            if fallback not in used_ids:

                used_ids.add(
                    fallback
                )

                return fallback

    # -------------------------------------------------
    # Migrate every record
    # -------------------------------------------------

    for row in rows:

        clip_id = choose_clip_id(
            row
        )

        video_id = value(
            row,
            "video_id"
        )

        audio = value(
            row,
            "audio"
        )

        if not isinstance(
            audio,
            (
                bytes,
                bytearray,
                memoryview
            )
        ):

            audio = None

        topic = value(
            row,
            "topic"
        )

        transcription = value(
            row,
            "transcription"
        )

        if transcription is None:

            transcription = value(
                row,
                "transcript"
            )

        # -------------------------------------------------
        # Word timestamps
        # -------------------------------------------------

        word_timestamps = value(
            row,
            "word_timestamps"
        )

        word_timestamps = (
            _format_word_timestamps(
                word_timestamps
            )
        )

        # -------------------------------------------------
        # KEEP DURATION
        # -------------------------------------------------

        duration = value(
            row,
            "duration"
        )

        verified = value(
            row,
            "verified",
            0
        )

        language = value(
            row,
            "language",
            "si"
        )

        created_at = value(
            row,
            "created_at"
        )

        # -------------------------------------------------
        # Insert into new table
        #
        # NOTE:
        # start_time and end_time are intentionally
        # NOT copied.
        # -------------------------------------------------

        cursor.execute(
            """
            INSERT INTO clips(
                clip_id,
                video_id,
                audio,
                topic,
                transcription,
                word_timestamps,
                duration,
                verified,
                language,
                created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clip_id,
                video_id,
                audio,
                topic,
                transcription,
                word_timestamps,
                duration,
                verified,
                language or "si",
                created_at
            )
        )

    # -------------------------------------------------
    # Delete old table
    # -------------------------------------------------

    cursor.execute(
        "DROP TABLE clips_old"
    )


# =====================================================
# CHECK DUPLICATE VIDEO
# =====================================================

def is_processed(video_id):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT video_id
        FROM videos
        WHERE video_id=?
        """,
        (video_id,)
    )

    result = cursor.fetchone()

    conn.close()

    return result is not None


# =====================================================
# SAVE VIDEO
# =====================================================

def save_video(
    video_id,
    title,
    url,
    duration
):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT OR IGNORE INTO videos(
            video_id,
            title,
            url,
            duration
        )
        VALUES(?,?,?,?)
        """,
        (
            video_id,
            title,
            url,
            duration
        )
    )

    conn.commit()
    conn.close()


# =====================================================
# MARK VIDEO DOWNLOADED
# =====================================================

def mark_downloaded(video_id):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE videos
        SET downloaded=1
        WHERE video_id=?
        """,
        (video_id,)
    )

    conn.commit()
    conn.close()


# =====================================================
# MARK VIDEO PROCESSED
# =====================================================

def mark_processed(video_id):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE videos
        SET processed=1
        WHERE video_id=?
        """,
        (video_id,)
    )

    conn.commit()
    conn.close()


# =====================================================
# GET NEXT CHUNK INDEX FROM DATABASE
# =====================================================

def get_next_chunk_index():

    conn = get_connection()
    cursor = conn.cursor()

    try:

        cursor.execute(
            """
            SELECT clip_id
            FROM clips
            WHERE clip_id LIKE 'chunk_%'
            """
        )

        rows = cursor.fetchall()

    finally:

        conn.close()

    indices = []

    for row in rows:

        clip_id = str(
            row[0]
        )

        match = re.match(
            r"^chunk_(\d+)(?:\.wav)?$",
            clip_id,
            re.IGNORECASE
        )

        if match:

            indices.append(
                int(match.group(1))
            )

    if not indices:

        print(
            "\nNo existing audio chunks in database."
        )

        print(
            "Starting chunk numbering from 1."
        )

        return 1

    highest = max(
        indices
    )

    next_index = (
        highest + 1
    )

    print(
        f"\nFound {len(indices)} audio chunks in database."
    )

    print(
        f"Highest chunk number: {highest}"
    )

    print(
        f"Next chunk number: {next_index}"
    )

    return next_index


# =====================================================
# SAVE COMPLETE CLIP
# =====================================================

def save_clip(
    clip_id,
    video_id,
    audio,
    topic,
    transcription,
    word_timestamps,
    duration,
    verified=0,
    language="si"
):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO clips(
            clip_id,
            video_id,
            audio,
            topic,
            transcription,
            word_timestamps,
            duration,
            verified,
            language
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            clip_id,
            video_id,
            sqlite3.Binary(audio),
            topic,
            transcription,
            word_timestamps,
            duration,
            verified,
            language
        )
    )

    conn.commit()
    conn.close()


# =====================================================
# GET CLIPS OF VIDEO
# =====================================================

def get_clips_by_video(video_id):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM clips
        WHERE video_id=?
        ORDER BY created_at
        """,
        (video_id,)
    )

    rows = cursor.fetchall()

    conn.close()

    return rows


# =====================================================
# SEARCH TRANSCRIPTS
# =====================================================

def search_transcripts(keyword):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM clips
        WHERE transcription LIKE ?
        """,
        (f"%{keyword}%",)
    )

    rows = cursor.fetchall()

    conn.close()

    return rows


# =====================================================
# GET ALL VIDEOS
# =====================================================

def get_all_videos():

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM videos"
    )

    rows = cursor.fetchall()

    conn.close()

    return rows


# =====================================================
# GET ALL CLIPS
# =====================================================

def get_all_clips():

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM clips"
    )

    rows = cursor.fetchall()

    conn.close()

    return rows