# =====================================================
# database.py
# =====================================================

import sqlite3

from config import DATABASE_NAME


# =====================================================
# DATABASE CONNECTION
# =====================================================

def get_connection():
    return sqlite3.connect(DATABASE_NAME)


# =====================================================
# CREATE DATABASE
# =====================================================

def create_database():

    conn = get_connection()
    cursor = conn.cursor()

    # -------------------------------------------------
    # VIDEOS TABLE
    # -------------------------------------------------

    cursor.execute("""

    CREATE TABLE IF NOT EXISTS videos(

        id INTEGER PRIMARY KEY AUTOINCREMENT,

        video_id TEXT UNIQUE,

        title TEXT,

        url TEXT,

        duration INTEGER,

        downloaded INTEGER DEFAULT 0,

        processed INTEGER DEFAULT 0,

        drive_file_id TEXT,

        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    )

    """)

    # -------------------------------------------------
    # CLIPS TABLE
    # -------------------------------------------------

    cursor.execute("""

    CREATE TABLE IF NOT EXISTS clips(

        clip_id INTEGER PRIMARY KEY AUTOINCREMENT,

        video_id TEXT NOT NULL,

        clip_name TEXT,

        start_time REAL,

        end_time REAL,

        duration REAL,

        transcript TEXT,

        verified INTEGER DEFAULT 0,

        language TEXT DEFAULT 'si',

        drive_file_id TEXT,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY(video_id)
        REFERENCES videos(video_id)

    )

    """)

    conn.commit()
    conn.close()


# =====================================================
# CHECK DUPLICATE VIDEO
# =====================================================

def is_processed(video_id):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT video_id FROM videos WHERE video_id=?",
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
# SAVE VIDEO DRIVE FILE ID
# =====================================================

def save_video_drive_file_id(
    video_id,
    drive_file_id
):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(

        """

        UPDATE videos

        SET drive_file_id=?

        WHERE video_id=?

        """,

        (
            drive_file_id,
            video_id
        )

    )

    conn.commit()
    conn.close()


# =====================================================
# SAVE CLIP
# =====================================================

def save_clip(

    video_id,
    clip_name,
    duration,
    start_time=None,
    end_time=None

):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(

        """

        INSERT INTO clips(

            video_id,
            clip_name,
            start_time,
            end_time,
            duration

        )

        VALUES(?,?,?,?,?)

        """,

        (

            video_id,
            clip_name,
            start_time,
            end_time,
            duration

        )

    )

    conn.commit()
    conn.close()
# =====================================================
# UPDATE TRANSCRIPT
# =====================================================

def update_clip_transcript(

    clip_name,
    transcript

):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(

        """

        UPDATE clips

        SET

            transcript=?,
            verified=1

        WHERE clip_name=?

        """,

        (
            transcript,
            clip_name
        )

    )

    conn.commit()
    conn.close()


# =====================================================
# SAVE CLIP DRIVE FILE ID
# =====================================================

def update_clip_drive_file_id(

    clip_name,
    drive_file_id

):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(

        """

        UPDATE clips

        SET drive_file_id=?

        WHERE clip_name=?

        """,

        (
            drive_file_id,
            clip_name
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

        ORDER BY start_time

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

        WHERE transcript LIKE ?

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