from pydub import AudioSegment
import os

def split_audio(file_path):
    os.makedirs("dataset/clips", exist_ok=True)

    # Load audio and IMMEDIATELY convert to 16kHz Mono
    audio = AudioSegment.from_wav(file_path)
    audio = audio.set_frame_rate(16000).set_channels(1)

    chunk_length_ms = 30000  # 30 seconds
    chunks = []

    for i in range(0, len(audio), chunk_length_ms):
        chunk = audio[i:i+chunk_length_ms]
        if len(chunk) < 2000:
            continue
            
        filename = f"dataset/clips/chunk_{i//chunk_length_ms:05d}.wav"
        # Export with specific parameters
        chunk.export(filename, format="wav", codec="pcm_s16le")
        chunks.append(filename)

    return chunks