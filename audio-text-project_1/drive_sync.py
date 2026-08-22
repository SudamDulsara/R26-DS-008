# =====================================================
# drive_sync.py
# =====================================================

import os

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from config import (
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_DRIVE_TOKEN
)


# =====================================================
# GOOGLE DRIVE SETTINGS
# =====================================================

SCOPES = [
    "https://www.googleapis.com/auth/drive"
]


# =====================================================
# GET GOOGLE DRIVE SERVICE
# =====================================================

def get_drive_service():

    if not os.path.exists(
        GOOGLE_DRIVE_TOKEN
    ):

        print(
            "❌ Google Drive token not found."
        )

        print(
            "Please run the Google Drive authentication "
            "setup first."
        )

        return None

    try:

        credentials = (
            Credentials.from_authorized_user_file(
                GOOGLE_DRIVE_TOKEN,
                SCOPES
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
            f"❌ Google Drive authentication error: {e}"
        )

        return None


# =====================================================
# CHECK IF FILE ALREADY EXISTS
# =====================================================

def file_exists(
    service,
    filename
):

    # -------------------------------------------------
    # Escape single quotes for Drive query
    # -------------------------------------------------

    escaped_filename = filename.replace(
        "'",
        "\\'"
    )

    query = (

        f"name = '{escaped_filename}' "

        f"and '{GOOGLE_DRIVE_FOLDER_ID}' in parents "

        "and trashed = false"

    )

    try:

        results = service.files().list(

            q=query,

            spaces="drive",

            fields="files(id,name)",

            pageSize=100

        ).execute()

        files = results.get(
            "files",
            []
        )

        return files[0] if files else None

    except Exception as e:

        print(
            f"❌ Error checking Drive file: {e}"
        )

        return None


# =====================================================
# UPLOAD FILE TO GOOGLE DRIVE
# =====================================================

def copy_to_drive(
    file_path
):

    # --------------------------------------------
    # CHECK LOCAL FILE
    # --------------------------------------------

    if not os.path.exists(
        file_path
    ):

        print(
            "❌ File not found."
        )

        return None

    # --------------------------------------------
    # CONNECT TO GOOGLE DRIVE
    # --------------------------------------------

    service = get_drive_service()

    if service is None:

        return None

    # --------------------------------------------
    # FILE NAME
    # --------------------------------------------

    filename = os.path.basename(
        file_path
    )

    # --------------------------------------------
    # CHECK DUPLICATE
    # --------------------------------------------

    existing_file = file_exists(

        service,

        filename

    )

    if existing_file:

        print()

        print(
            "=" * 60
        )

        print(
            "✓ File already exists in Google Drive"
        )

        print(
            f"File: {filename}"
        )

        print(
            f"Drive ID: {existing_file['id']}"
        )

        print(
            "=" * 60
        )

        # Return existing Drive ID
        return existing_file["id"]

    # --------------------------------------------
    # FILE METADATA
    # --------------------------------------------

    file_metadata = {

        "name": filename,

        "parents": [
            GOOGLE_DRIVE_FOLDER_ID
        ]

    }

    # --------------------------------------------
    # UPLOAD
    # --------------------------------------------

    try:

        media = MediaFileUpload(

            file_path,

            mimetype="audio/wav",

            resumable=True

        )

        uploaded_file = service.files().create(

            body=file_metadata,

            media_body=media,

            fields="id,name,webViewLink"

        ).execute()

        print()

        print(
            "=" * 60
        )

        print(
            "✓ Uploaded to Google Drive"
        )

        print(
            f"File: {uploaded_file['name']}"
        )

        print(
            f"Drive ID: {uploaded_file['id']}"
        )

        print(
            f"URL: "
            f"https://drive.google.com/file/d/"
            f"{uploaded_file['id']}/view"
        )

        print(
            "=" * 60
        )

        # Return new Drive ID
        return uploaded_file["id"]

    except Exception as e:

        print()

        print(
            f"❌ Google Drive upload failed: {e}"
        )

        return None


# =====================================================
# TEST
# =====================================================

if __name__ == "__main__":

    drive_file_id = copy_to_drive(

        "dataset/clips/chunk_00001.wav"

    )

    print(
        f"\nReturned Drive File ID: "
        f"{drive_file_id}"
    )