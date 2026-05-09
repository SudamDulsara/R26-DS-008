"""
Download the Sinhala Wikipedia corpus from HuggingFace and save as plain text.

Source  : wikimedia/wikipedia
Config  : 20231101.si
Articles: ~23,065 Sinhala Wikipedia articles (~54 MB)
Output  : data/sinhala_corpus.txt
"""

from datasets import load_dataset

print("Downloading wikimedia/wikipedia (20231101.si) from HuggingFace...")
print("This may take a few minutes on first run (~54 MB).")

ds = load_dataset("wikimedia/wikipedia", "20231101.si", split="train", trust_remote_code=True)

print(f"Downloaded {len(ds):,} articles.")

output_path = "data/sinhala_corpus.txt"

with open(output_path, "w", encoding="utf-8") as f:
    for article in ds:
        text = article["text"].strip()
        if text:
            f.write(text + "\n\n")

print(f"Saved → {output_path}")
print(f"Articles written: {len(ds):,}")
