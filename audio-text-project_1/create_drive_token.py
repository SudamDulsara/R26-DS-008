# =========================================================
# create_drive_token.py
# =========================================================

from google_auth_oauthlib.flow import InstalledAppFlow
import os


# =========================================================
# GOOGLE DRIVE SETTINGS
# =========================================================

SCOPES = [
    "https://www.googleapis.com/auth/drive"
]


# OAuth credentials
CREDENTIALS_FILE = r"credentials\audio.json"


# Token for this computer/user
TOKEN_FILE = r"token.json"


# =========================================================
# START
# =========================================================

print("=" * 60)
print("GOOGLE DRIVE AUTHENTICATION")
print("=" * 60)


# =========================================================
# CHECK CREDENTIALS
# =========================================================

print("\nChecking credentials file...")

if not os.path.exists(CREDENTIALS_FILE):

    print("❌ audio.json not found!")

    print(
        "Expected location:"
    )

    print(
        os.path.abspath(CREDENTIALS_FILE)
    )

    raise FileNotFoundError(
        f"Could not find {CREDENTIALS_FILE}"
    )


print("✅ audio.json found.")


# =========================================================
# GOOGLE AUTHENTICATION
# =========================================================

print("\nOpening Google authorization...")

flow = InstalledAppFlow.from_client_secrets_file(
    CREDENTIALS_FILE,
    SCOPES
)

credentials = flow.run_local_server(
    port=0
)


# =========================================================
# SAVE TOKEN
# =========================================================

print("\nSaving Google Drive token...")

with open(
    TOKEN_FILE,
    "w"
) as token:

    token.write(
        credentials.to_json()
    )


# =========================================================
# VERIFY
# =========================================================

if os.path.exists(TOKEN_FILE):

    print("\n" + "=" * 60)
    print("✅ GOOGLE DRIVE AUTHENTICATION SUCCESSFUL")
    print("=" * 60)

    print(
        "\nToken saved to:"
    )

    print(
        os.path.abspath(TOKEN_FILE)
    )

else:

    print(
        "\n❌ Token could not be created."
    )