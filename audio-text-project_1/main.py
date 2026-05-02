import os
import time
import csv
import yt_dlp
from segment import split_audio
from transcribe import transcribe_audio

def download_audio(url):
    """Downloads audio from YouTube and converts it to WAV."""
    print(f"📥 Downloading: {url}")
    
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'wav',
            'preferredquality': '192',
        }],
        'outtmpl': 'downloaded_audio', 
        'quiet': True,
    }
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])
    
    expected_file = "downloaded_audio.wav"
    # Handling potential double extension issues
    if not os.path.exists(expected_file):
        if os.path.exists("downloaded_audio.wav.wav"):
            os.rename("downloaded_audio.wav.wav", expected_file)
        else:
            for f in os.listdir('.'):
                if f.startswith("downloaded_audio") and f.endswith(".wav"):
                    os.rename(f, expected_file)
                    break
    return expected_file

def clean_text(text):
    """Basic cleaning of Sinhala text."""
    if not text: return ""
    return text.strip().replace("\n", " ").replace("*", "")

def main():
    video_url = input("Enter YouTube URL: ")
    
    # 1. Download
    try:
        audio_file = download_audio(video_url)
    except Exception as e:
        print(f"❌ Download Failed: {e}")
        return

    # 2. Segment
    print("✂️ Segmenting audio into 30s chunks...")
    chunks = split_audio(audio_file)
    if not chunks:
        print("❌ No chunks found.")
        return

    # 3. Transcribe
    dataset_rows = []
    print(f"🎙️ Transcribing {len(chunks)} chunks locally...")
    
    for i, chunk in enumerate(chunks):
        print(f"[{i+1}/{len(chunks)}] Processing: {chunk}")
        
        raw_text = transcribe_audio(chunk)
        final_text = clean_text(raw_text)

        if len(final_text) > 2:
            dataset_rows.append({
                "path": chunk,
                "text": final_text,
                "duration": 30.0
            })
            print(f"✅ Text: {final_text[:50]}...")
        else:
            print(f"⚠️ No text found for {chunk}")

    # 4. Save to TSV
    save_path = "train.tsv"
    file_exists = os.path.isfile(save_path)
    
    # utf-8-sig ensures Sinhala looks correct in Excel/Notepad
    with open(save_path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["path", "text", "duration"], delimiter="\t")
        if not file_exists:
            writer.writeheader()
        writer.writerows(dataset_rows)

    print(f"\n🚀 Complete! {len(dataset_rows)} rows added to {save_path}")

if __name__ == "__main__":
    main()