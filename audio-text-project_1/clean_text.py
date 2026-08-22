import re

def clean_text(text):
    """
    Advanced cleaning for Component 4.
    Targets hallucinations and formatting noise found in YouTube ASR.
    """
    if not text:
        return ""

    # 1. Basic Cleaning
    # Remove newlines, asterisks, and extra whitespace
    text = text.strip().replace("\n", " ").replace("*", "")

    # 2. Handle Phonetic/ASR Hallucinations
    # Remove phrases that Whisper often 'invents' when it hears music or silence
    hallucinations = [
        "Subtitles by", "Please subscribe", "thank you for watching",
        "අන් එහෙම්මයි", # Example: remove filler phrases that don't match ground truth
    ]
    for phrase in hallucinations:
        # Case insensitive replacement for English phrases
        text = re.sub(phrase, "", text, flags=re.IGNORECASE)

    # 3. Remove Repetitive Characters
    # Whisper often repeats characters when it gets stuck on background noise
    # Example: "අවශ්‍යකාබබබබවට" -> "අවශ්‍යතාවට"
    # This regex finds any character repeated 3 or more times and reduces it to 1
    text = re.sub(r'(.)\1{2,}', r'\1', text)

    # 4. Remove Non-Sinhala/Non-Punctuation
    # Keep only Sinhala Unicode range, numbers, and basic punctuation
    # This helps eliminate random English characters or symbols produced by errors
    text = re.sub(r'[^\u0D80-\u0DFF0-9\s.,?]', '', text)

    # 5. Final whitespace normalization
    text = " ".join(text.split())

    return text