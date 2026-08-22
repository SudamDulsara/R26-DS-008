# =====================================================
# download.py
# =====================================================

import os
import yt_dlp

from database import mark_downloaded

# =====================================================
# DOWNLOAD AUDIO
# =====================================================

def download_audio(video):

    """
    Downloads the audio of the selected
    YouTube video and updates the database.
    """

    os.makedirs(
        "temp",
        exist_ok=True
    )

    output_template = os.path.join(
        "temp",
        "audio.%(ext)s"
    )

    output_audio = os.path.join(
        "temp",
        "audio.wav"
    )

    ydl_opts = {

        "format": "bestaudio/best",

        "outtmpl": output_template,

        "quiet": False,

        "postprocessors": [

            {

                "key": "FFmpegExtractAudio",

                "preferredcodec": "wav",

                "preferredquality": "192"

            }

        ]

    }

    print("\nDownloading audio...")
    print(f"Title : {video['title']}")

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:

        ydl.download([video["url"]])

    # ----------------------------------------------
    # UPDATE DATABASE
    # ----------------------------------------------

    mark_downloaded(
        video["video_id"]
    )

    print("Download Complete.")

    return output_audio


# =====================================================
# TEST
# =====================================================

if __name__ == "__main__":

    from collector import get_next_video

    video = get_next_video()

    if video:

        audio = download_audio(video)

        print("\nDownloaded File:")
        print(audio)

    else:

        print("No videos available.")