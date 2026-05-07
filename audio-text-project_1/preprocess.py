from pydub import AudioSegment
import os

def preprocess_audio(input_path):
    """Normalize audio levels to improve transcription accuracy."""
    if not os.path.exists(input_path): return input_path
    
    audio = AudioSegment.from_file(input_path)
    # Normalize to -20dBFS
    normalized_audio = audio.normalize()
    
    output_path = "temp_clean.wav"
    normalized_audio.export(output_path, format="wav")
    return output_path