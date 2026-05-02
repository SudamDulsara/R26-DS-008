from pydub import AudioSegment
import os

def preprocess_audio(input_path):
    audio = AudioSegment.from_wav(input_path)

    # Normalize volume
    audio = audio.normalize()

    output_path = "temp/clean.wav"
    audio.export(output_path, format="wav")

    return output_path