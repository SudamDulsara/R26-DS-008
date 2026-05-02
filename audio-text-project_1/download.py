import yt_dlp
import os

def download_audio(url):
    os.makedirs("temp", exist_ok=True)

    output_path = "temp/audio.wav"

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': 'temp/audio.%(ext)s',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'wav',
        }],
        'quiet': True
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

    return output_path