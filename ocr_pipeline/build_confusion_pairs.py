"""
Analyse Sinhala Wikipedia corpus to find visually similar
character pairs that commonly appear as OCR substitutions.
"""

import re
import json
from collections import defaultdict

# Load wordlist
with open("data/sinhala_wordlist.json", "r", encoding="utf-8") as f:
    wordlist = set(json.load(f).keys())

print(f"Loaded {len(wordlist):,} words")

# Known visually similar Sinhala character pairs
# Format: (char_a, char_b) — both directions tested
VISUAL_PAIRS = [
    ("ව", "ච"),   # va  / ca
    ("ම", "න"),   # ma  / na
    ("ද", "ධ"),   # da  / dha
    ("ප", "ෂ"),   # pa  / sha
    ("ක", "ඛ"),   # ka  / kha
    ("ට", "ඨ"),   # tta / ttha
    ("ග", "ඝ"),   # ga  / gha
    ("ත", "ථ"),   # ta  / tha
    ("ඩ", "ඪ"),   # dda / ddha
    ("බ", "භ"),   # ba  / bha
    ("ල", "ළ"),   # la  / lla
    ("ස", "ශ"),   # sa  / sha
    ("ශ", "ෂ"),   # sha / ssa
    ("ණ", "න"),   # nna / na
    ("ඳ", "ද"),   # soft da
    ("ය", "ව"),   # ya  / va
]

# For each pair find words where swapping creates another valid word
confusion_map = defaultdict(list)
pair_stats    = []

print("Analysing confusion pairs...")

for char_a, char_b in VISUAL_PAIRS:
    ab_count = 0
    ba_count = 0

    for word in wordlist:
        # Try replacing char_a with char_b
        if char_a in word:
            candidate = word.replace(char_a, char_b)
            if candidate in wordlist and candidate != word:
                confusion_map[word].append(candidate)
                ab_count += 1

        # Try replacing char_b with char_a
        if char_b in word:
            candidate = word.replace(char_b, char_a)
            if candidate in wordlist and candidate != word:
                confusion_map[word].append(candidate)
                ba_count += 1

    total = ab_count + ba_count
    pair_stats.append({
        "pair":    f"{char_a} ↔ {char_b}",
        "char_a":  char_a,
        "char_b":  char_b,
        "a_to_b":  ab_count,
        "b_to_a":  ba_count,
        "total":   total
    })
    print(f"  {char_a} ↔ {char_b}: {total} ambiguous word pairs")

# Sort by most ambiguous
pair_stats.sort(key=lambda x: x["total"], reverse=True)

# Save results
with open("data/confusion_pairs.json", "w", encoding="utf-8") as f:
    json.dump({
        "pair_stats":    pair_stats,
        "confusion_map": {k: list(set(v)) for k, v in confusion_map.items()}
    }, f, ensure_ascii=False, indent=2)

print(f"\nTotal ambiguous words found: {len(confusion_map):,}")
print("Saved → data/confusion_pairs.json")
print("\nTop 5 most ambiguous pairs:")
for p in pair_stats[:5]:
    print(f"  {p['pair']}: {p['total']} pairs")