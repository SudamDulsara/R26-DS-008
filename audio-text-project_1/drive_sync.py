# =====================================================
# drive_sync.py
# =====================================================

import os
import shutil

from config import GOOGLE_DRIVE_FOLDER

# =====================================================
# COPY FILE TO GOOGLE DRIVE
# =====================================================

def copy_to_drive(file_path):

    # --------------------------------------------
    # CHECK FILE EXISTS
    # --------------------------------------------

    if not os.path.exists(file_path):

        print("❌ File not found.")

        return False

    # --------------------------------------------
    # CREATE DRIVE FOLDER IF NEEDED
    # --------------------------------------------

    os.makedirs(
        GOOGLE_DRIVE_FOLDER,
        exist_ok=True
    )

    # --------------------------------------------
    # DESTINATION
    # --------------------------------------------

    destination = os.path.join(

        GOOGLE_DRIVE_FOLDER,

        os.path.basename(file_path)

    )

    # --------------------------------------------
    # SKIP IF FILE ALREADY EXISTS
    # --------------------------------------------

    if os.path.exists(destination):

        print()

        print("=" * 60)

        print("✓ File already exists in Google Drive")

        print(destination)

        print("=" * 60)

        return True

    # --------------------------------------------
    # COPY FILE
    # --------------------------------------------

    shutil.copy2(

        file_path,

        destination

    )

    print()

    print("=" * 60)

    print("✓ Copied to Google Drive")

    print(destination)

    print("=" * 60)

    return True


# =====================================================
# TEST
# =====================================================

if __name__ == "__main__":

    copy_to_drive(

        "dataset/clips/chunk_00001.wav"

    )