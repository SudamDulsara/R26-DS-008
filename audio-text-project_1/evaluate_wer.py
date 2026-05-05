import jiwer
import pandas as pd

def calculate_metrics(reference_text, hypothesis_text):
    # Standardizes text and calculates Word Error Rate
    wer = jiwer.wer(reference_text, hypothesis_text)
    return wer

# Example for your PP1 Table
data = {
    "Source": ["SLR52 (Standard)", "YouTube (Your Pipeline)"],
    "Reference": ["මම අද පාසල් ගියෙමි", "මම අද පාසල් ගියෙමි"], # What was actually said
    "Hypothesis": ["මම අද පාසල් ගියෙමි", "මම අද පාසල ගියා"]   # What the model heard
}

for i in range(len(data["Source"])):
    score = calculate_metrics(data["Reference"][i], data["Hypothesis"][i])
    print(f"Dataset: {data['Source'][i]} | WER: {score:.2%}")