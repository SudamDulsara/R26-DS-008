import os

# IMPORTANT: Set the cache directory to E drive BEFORE importing transformers
# This ensures the 1GB model is NOT saved on your full C drive.
os.environ['HF_HOME'] = 'E:/huggingface_cache'

import torch
from transformers import pipeline

# Check if GPU is available
device = "cuda:0" if torch.cuda.is_available() else "cpu"
model_id = "Lingalingeswaran/whisper-small-sinhala"

print(f"📡 Loading Model: {model_id}")
print(f"📂 Saving model data to: E:/huggingface_cache")
print(f"💻 Running on: {device.upper()}")

# Initialize the pipeline
try:
    # This will now download the model to E:/huggingface_cache/hub
    transcriber = pipeline(
        "automatic-speech-recognition", 
        model=model_id, 
        device=device
    )
except Exception as e:
    print(f"❌ Error loading model: {e}")

def transcribe_audio(file_path):
    """Transcribes a 30s audio chunk using local Whisper model."""
    try:
        if not os.path.exists(file_path):
            print(f"⚠️ File not found: {file_path}")
            return ""

        # Run transcription with Sinhala language settings
        result = transcriber(
            file_path, 
            generate_kwargs={"language": "sinhalese"}
        )
        
        return result["text"].strip()
    except Exception as e:
        print(f"❌ Transcription error on {file_path}: {e}")
        return ""