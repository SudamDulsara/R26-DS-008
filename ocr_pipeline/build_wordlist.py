import re
from collections import Counter
import json

print("Reading corpus...")
with open("data/sinhala_corpus.txt", "r", encoding="utf-8") as f:
    text = f.read()

# Extract only Sinhala words (Unicode block U+0D80–U+0DFF)
print("Extracting Sinhala words...")
words = re.findall(r'[\u0D80-\u0DFF]+', text)

print(f"Total word tokens found: {len(words):,}")

# Count frequencies
freq = Counter(words)

# Keep only words that appear at least 3 times
# (removes noise and rare OCR artifacts)
filtered = {w: c for w, c in freq.items() if c >= 3 and len(w) >= 2}

print(f"Unique words (freq >= 3): {len(filtered):,}")

# Save as JSON
with open("data/sinhala_wordlist.json", "w", encoding="utf-8") as f:
    json.dump(filtered, f, ensure_ascii=False, indent=2)

print("Saved → sinhala_wordlist.json")

# Show top 20 most common words
print("\nTop 20 most frequent words:")
for word, count in freq.most_common(20):
    print(f"  {word}: {count}")