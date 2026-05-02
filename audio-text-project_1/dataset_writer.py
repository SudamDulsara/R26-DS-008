import pandas as pd
from pydub import AudioSegment
import os

def get_duration(file_path):
    audio = AudioSegment.from_wav(file_path)
    return len(audio) / 1000

def save_dataset(rows):
    os.makedirs("dataset", exist_ok=True)

    df = pd.DataFrame(rows)

    file_path = "dataset/train.tsv"

    if os.path.exists(file_path):
        df.to_csv(file_path, sep="\t", index=False, mode='a', header=False)
    else:
        df.to_csv(file_path, sep="\t", index=False)