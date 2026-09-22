# =====================================================
# ADVANCED SINHALA TEXT CORRECTION SYSTEM
# =====================================================

import re
import unicodedata

from difflib import SequenceMatcher
from difflib import get_close_matches

# =====================================================
# COMMON CORRECTIONS
# =====================================================

COMMON_CORRECTIONS = {

    # -------------------------------------------------
    # GENERAL ASR ERRORS
    # -------------------------------------------------

    "කථාව": "කතාව",
    "ජීවිදය": "ජීවිතය",
    "උගුස්සා": "උකුස්සා",
    "සක්තිය": "ශක්තිය",
    "බාෂාව": "භාෂාව",
    "දෑග්ගක්": "තෑග්ගක්",
    "මාරගහ": "මාර ගහ",
    "හිසිම": "කිසිම",
    "දුබල": "දුර්වල",
    "කූලග": "කුළඟ",
    "උණපඟුර": "උණ පඳුර",
    "ශර්ථාය": "ශක්තිය",
    "ඉතිං": "ඉතින්",
    "පස්සේ": "පස්සෙ",

    # -------------------------------------------------
    # DATASET ERRORS
    # -------------------------------------------------

    "අක්සේර්": "අක්ෂර",
    "කහනවා": "අහනවා",
    "ලේෂ": "රේස්",
    "ගිහිල්ල": "ගිහිල්ලා",
    "අපාසුතාවකින්": "අපහසුතාවයකින්",
    "සවන්දෙවින්": "සවන් දෙමින්",
    "වෙහිත": "වෙයිද",
    "අතාරින්න": "අත්හරින්න",
    "යළුත්": "අලුත්",
    "හිනාවලා": "හිනාවෙලා",
    "දුවන්": "දුවන්න",
    "නැකිටින්න": "නැගිටින්න",

    # -------------------------------------------------
    # COMMON SPOKEN FORMS
    # -------------------------------------------------

    "ගන්නෙක": "ගන්න එක",
    "කරන්නෙක": "කරන එක",
    "තියනවා": "තියෙනවා",
    "මිනිස්සුව": "මිනිස්සු",
    "අමතක්": "අමතක",
    "කියලක්": "කියලා",
    "කරනව": "කරනවා",
    "ඉන්නව": "ඉන්නවා",
    "ගියෙ": "ගියේ",
    "එන්නෙ": "එන්නේ",
    "ඉන්නෙ": "ඉන්නේ",
    "දෙන්නෙ": "දෙන්නේ",

    # -------------------------------------------------
    # BROKEN WORDS
    # -------------------------------------------------

    "අතිශිඉසිතිරව": "අතිශයින් ස්ථිරව",
    "නිසැකෝම": "නිසැකවම",
    "වතුර්": "වතුර",
    "වතුරු": "වතුර",
    "සක්තු": "සත්තු",
    "සත්වූ": "සත්තු",
    "මුකක්ද": "මොකක්ද",
    "මොකද්ද": "මොකක්ද",
    "පුළුවම්වෙයි": "පුළුවන් වෙයි",
    "නිවන්ඩ": "නිවන්න",
    "පොටන්ගන්න": "පටන්ගන්න",
    "හැක්යාව": "හැකියාව",
    "හතාව": "කතාව",
    "දහි": "දැයි",
    "යාදහස": "අදහස",
    "සක්පාර්තනා": "සුභ පැතුම්",
    "චෑග්‍රහි": "ජයග්‍රාහී",
    "මෙමයි": "මෙහෙමයි",
    "වනාන්තරයෙක": "වනාන්තරයක",
    "පියාමගෙන": "පියාඹගෙන",
    "ඇලා": "එනවා",
    "උදවුවෙන්න": "උදව් වෙන්න",
    "හොඩවල්": "හොඬවැල්",
    "කොලාතු": "කොළ අතු",
    "ප්තා": "පවා",
    "එරම්": "තරම්",
    "තෙන්නෙ": "හිතෙන්නෙ",
    "වේවාන්": "වේවා",
    "චුටි": "පුංචි",
    "නිර්බිත": "නිර්භීත",
    "මෙක්": "මේක",
    "එහෙමෙහි": "එහෙ මෙහෙ",
    "දුවක්": "දුවනවා",
    "ගහගා": "පියාඹා",
    "තියාම": "දිහා",
    "කුරුල්ලට": "කුරුල්ලාට",
    "නිර්මීත": "නිර්භීත",
    "තමතමන්ට": "තම තමන්ට",
    "මැදිහත්": "මැදිහත් වෙලා",
    "දෙදීට": "දේට",
    "අහගන": "අහගෙන",
    "අනිත්": "අනෙක්",
    "අත්‍යවශ්‍වේ": "අත්‍යවශ්‍ය",
    "පියවරවනතර": "පියවර වන අතර",
    "ජයග්‍රහණයයි":"ජයග්‍රහණයේ",
    "අතිශිචිචව":"අතිශයින්",
    "අත්යවශ්ය":"අත්‍යවශ්‍ය",
    "සැම විටම":" සැමවිටම",
    "ලියඩ":"ලෙඩ",
    " අවශ්යතාවයන්":"අවශ්‍යතාවයන්",
    "රටයන්නලු ":"රට යන්නලු",
    "ග්රලර්ලක්":"ඩොලර්",
    "කියන්දී":"තියෙන්නේ.",
    "සහිබ":"සයිබර්",
    "වුවත්":"පුවත්",
    "සබ්ස්බ්රයිප්":"සබ්ස්ක්‍රයිබ්",
    "සමක්":"සමඟ",
    "දෙමල":"දෙමළ",
    "වසම්වකරයිප්":"සබ්ස්ක්‍රයිබ්",


    # -------------------------------------------------
    # MIXED LANGUAGE NORMALIZATION
    # -------------------------------------------------

    # Keep these in Sinhala script.  The dataset is intended
    # to contain Sinhala speech/text, so English spellings are
    # not introduced by the correction stage.
    "ඇන්ඩ්රොයිඩ්": "ඇන්ඩ්රොයිඩ්",
    "ඇන්ඩ්‍රොයිඩ්": "ඇන්ඩ්‍රොයිඩ්",
    "මැගසින්ස්": "මැගසින්ස්",
    "මැගසීන්ස්": "මැගසීන්ස්",

    # -------------------------------------------------
    # SPACE NORMALIZATION
    # -------------------------------------------------

    "ඔබතුළ": "ඔබ තුළ",
    "සැම විටම": "සැමවිටම",
    "විශ්වාස": "විශ්වාසය"
    ""
}

# =====================================================
# PHONETIC GROUPS
# =====================================================

PHONETIC_GROUPS = {

    "ශ": "ස",
    "ෂ": "ස",
    "ස": "ස",

    "බ": "බ",
    "භ": "බ",

    "ද": "ද",
    "ධ": "ද",

    "ත": "ත",
    "ථ": "ත",

    "ට": "ත",

    "ක": "ක",
    "ඛ": "ක",

    "ග": "ග",
    "ඝ": "ග",

    "ජ": "ජ",
    "ඣ": "ජ",

    "ච": "ච",
    "ඡ": "ච",

    "ළ": "ල",
    "ල": "ල",

    "ණ": "න",
    "න": "න"
}

# =====================================================
# UNICODE NORMALIZATION
# =====================================================

def normalize_unicode(text):

    return unicodedata.normalize(
        "NFC",
        text
    )

# =====================================================
# REMOVE REPEATED CHARACTERS
# =====================================================

def remove_repeated_characters(text):

    return re.sub(
        r'(.)\1{2,}',
        r'\1',
        text
    )

# =====================================================
# REMOVE REPEATED WORDS
# =====================================================

def remove_duplicate_words(text):

    words = text.split()

    cleaned = []

    previous = ""

    for word in words:

        if word != previous:

            cleaned.append(word)

        previous = word

    return " ".join(cleaned)

# =====================================================
# REMOVE NOISE SYMBOLS
# =====================================================

def clean_symbols(text):

    return re.sub(

        r'[^අ-෴a-zA-Z0-9\s.,!?-]',

        '',

        text
    )

# =====================================================
# NORMALIZE SPACES
# =====================================================

def normalize_spaces(text):

    text = re.sub(
        r'\s+',
        ' ',
        text
    )

    return text.strip()

# =====================================================
# FIX PUNCTUATION
# =====================================================

def fix_punctuation_spacing(text):

    text = re.sub(
        r'\s+([.,!?])',
        r'\1',
        text
    )

    text = re.sub(
        r'([.,!?])([^\s])',
        r'\1 \2',
        text
    )

    return text

# =====================================================
# PHONETIC NORMALIZATION
# =====================================================

def phonetic_normalize(word):

    normalized = ""

    for char in word:

        normalized += PHONETIC_GROUPS.get(
            char,
            char
        )

    return normalized

# =====================================================
# SIMILARITY SCORE
# =====================================================

def similarity(a, b):

    return SequenceMatcher(
        None,
        a,
        b
    ).ratio()

# =====================================================
# ADVANCED PHONETIC MATCHING
# =====================================================

def phonetic_similarity_correction(word):

    # -------------------------------------------------
    # DIRECT MATCH
    # -------------------------------------------------

    if word in COMMON_CORRECTIONS:

        return COMMON_CORRECTIONS[word]

    normalized_word = phonetic_normalize(
        word
    )

    best_match = None

    best_score = 0

    for candidate in COMMON_CORRECTIONS.keys():

        normalized_candidate = (
            phonetic_normalize(candidate)
        )

        score = similarity(

            normalized_word,

            normalized_candidate
        )

        # ---------------------------------------------
        # LENGTH BONUS
        # ---------------------------------------------

        if abs(len(word) - len(candidate)) <= 2:

            score += 0.05

        if score > best_score:

            best_score = score

            best_match = candidate

    # -------------------------------------------------
    # THRESHOLD
    # -------------------------------------------------

    if best_score >= 0.84:

        return COMMON_CORRECTIONS[
            best_match
        ]

    return word

# =====================================================
# MORPHOLOGICAL NORMALIZATION
# =====================================================

def normalize_suffixes(word):

    suffix_rules = {

        "න්නෙ": "න්නේ",
        "නව": "නවා",
        "වෙයිද": "වෙයි ද",
        "කියල": "කියලා",
        "විතරයිනේ": "විතරයි නේ"
    }

    for wrong, correct in suffix_rules.items():

        if word.endswith(wrong):

            word = word.replace(
                wrong,
                correct
            )

    return word

# =====================================================
# ENGLISH NORMALIZATION
# =====================================================

def normalize_english_terms(word):

    # -------------------------------------------------
    # PURE-SINHALA DATASET POLICY
    # -------------------------------------------------
    #
    # Earlier versions converted English terms such as
    # "youtube" -> "YouTube".  That is unsuitable for the
    # Sinhala audio-text dataset because it preserves English
    # content instead of filtering it.
    #
    # English content is now rejected by the validation stage.
    # This function therefore never introduces English text.
    # -------------------------------------------------

    return word


# =====================================================
# APPLY WORD CORRECTIONS
# =====================================================

def apply_dictionary_corrections(text):

    words = text.split()

    corrected_words = []

    for word in words:

        word = word.strip()

        # ---------------------------------------------
        # ENGLISH NORMALIZATION
        # ---------------------------------------------

        word = normalize_english_terms(
            word
        )

        # ---------------------------------------------
        # SUFFIX NORMALIZATION
        # ---------------------------------------------

        word = normalize_suffixes(
            word
        )

        # ---------------------------------------------
        # PHONETIC CORRECTION
        # ---------------------------------------------

        corrected = (
            phonetic_similarity_correction(
                word
            )
        )

        corrected_words.append(
            corrected
        )

    return " ".join(corrected_words)

# =====================================================
# SINHALA QUALITY / ENGLISH DETECTION
# =====================================================

def get_sinhala_quality(text):
    """
    Return Sinhala/English statistics for quality control.

    The dataset policy is intentionally strict: English words
    are treated as mixed-language noise rather than being
    deleted from an otherwise valid sentence.
    """

    if not text:
        return {
            "sinhala_ratio": 0.0,
            "english_ratio": 0.0,
            "english_tokens": [],
            "sinhala_chars": 0,
            "english_chars": 0,
            "other_letter_chars": 0,
            "letter_chars": 0,
        }

    sinhala_chars = len(
        re.findall(r"[\u0D80-\u0DFF]", text)
    )

    english_chars = len(
        re.findall(r"[A-Za-z]", text)
    )

    other_letter_chars = 0

    for char in text:
        if not unicodedata.category(char).startswith("L"):
            continue

        if re.match(r"[\u0D80-\u0DFF]", char):
            continue

        if re.match(r"[A-Za-z]", char):
            continue

        other_letter_chars += 1

    letter_chars = (
        sinhala_chars
        + english_chars
        + other_letter_chars
    )

    english_tokens = re.findall(
        r"(?<![A-Za-z])[A-Za-z]+(?:'[A-Za-z]+)?(?![A-Za-z])",
        text
    )

    if letter_chars == 0:
        sinhala_ratio = 0.0
        english_ratio = 0.0
    else:
        sinhala_ratio = sinhala_chars / letter_chars
        english_ratio = english_chars / letter_chars

    return {
        "sinhala_ratio": sinhala_ratio,
        "english_ratio": english_ratio,
        "english_tokens": english_tokens,
        "sinhala_chars": sinhala_chars,
        "english_chars": english_chars,
        "other_letter_chars": other_letter_chars,
        "letter_chars": letter_chars,
    }


def validate_sinhala_text(
    text,
    min_sinhala_ratio=0.82,
    allow_english_tokens=0,
):
    """
    Strict Sinhala validation for dataset admission.

    IMPORTANT:
        Mixed-language audio is rejected as a whole clip.
        English words are NOT removed from the transcript because
        doing so would make the text no longer match the audio.
    """

    metrics = get_sinhala_quality(text)

    if metrics["sinhala_chars"] == 0:
        return False, metrics

    if metrics["english_tokens"]:
        if len(metrics["english_tokens"]) > allow_english_tokens:
            return False, metrics

    # Reject other writing systems as well (for example Tamil).
    if metrics["other_letter_chars"] > 0:
        return False, metrics

    if metrics["sinhala_ratio"] < min_sinhala_ratio:
        return False, metrics

    return True, metrics


# =====================================================
# CORRECT ONE WORD WITHOUT CHANGING TIMESTAMP COUNT
# =====================================================

def correct_word_preserving_alignment(word):
    """
    Correct a single ASR word while preserving one word = one
    timestamp entry.

    If a correction would split one word into multiple words,
    the original word is retained so word-level timestamps do
    not become misaligned.
    """

    if word is None:
        return ""

    original = str(word).strip()
    if not original:
        return ""

    corrected = normalize_english_terms(original)
    corrected = normalize_suffixes(corrected)
    corrected = phonetic_similarity_correction(corrected)

    if not corrected or len(corrected.split()) != 1:
        return original

    return corrected


# =====================================================
# REMOVE SHORT NOISE TOKENS
# =====================================================

def remove_noise_tokens(text):

    words = text.split()

    cleaned = []

    for word in words:

        # keep english abbreviations
        if re.match(r'^[A-Za-z]+$', word):

            cleaned.append(word)

            continue

        # remove tiny garbage tokens
        if len(word) == 1:

            continue

        cleaned.append(word)

    return " ".join(cleaned)

# =====================================================
# FINAL CORRECTION PIPELINE
# =====================================================

def correct_text(text):

    # -------------------------------------------------
    # UNICODE NORMALIZATION
    # -------------------------------------------------

    text = normalize_unicode(text)

    # -------------------------------------------------
    # LOWERCASE ENGLISH
    # -------------------------------------------------

    text = text.strip()

    # -------------------------------------------------
    # REMOVE REPEATED LETTERS
    # -------------------------------------------------

    text = remove_repeated_characters(
        text
    )

    # -------------------------------------------------
    # CLEAN SYMBOLS
    # -------------------------------------------------

    text = clean_symbols(text)

    # -------------------------------------------------
    # REMOVE GARBAGE TOKENS
    # -------------------------------------------------

    text = remove_noise_tokens(text)

    # -------------------------------------------------
    # APPLY ADVANCED CORRECTIONS
    # -------------------------------------------------

    text = apply_dictionary_corrections(
        text
    )

    # -------------------------------------------------
    # REMOVE DUPLICATE WORDS
    # -------------------------------------------------

    text = remove_duplicate_words(
        text
    )

    # -------------------------------------------------
    # NORMALIZE SPACES
    # -------------------------------------------------

    text = normalize_spaces(text)

    # -------------------------------------------------
    # FIX PUNCTUATION
    # -------------------------------------------------

    text = fix_punctuation_spacing(
        text
    )

    # Do not let the correction stage introduce English into
    # a Sinhala-only dataset.  The caller decides whether to
    # reject the complete clip based on validation.
    return text

# =====================================================
# CLI TESTING
# =====================================================

if __name__ == "__main__":

    samples = [

        "සක්තිය ගොඩාක් වටිනව",
        "මොකද්ද මේ කරන්නෙක",
        "ඇන්ඩ්රොයිඩ් phone එක",
        "නිර්මීත කතාවක්"
    ]

    for s in samples:

        print("\nINPUT:")
        print(s)

        print("\nOUTPUT:")
        print(correct_text(s))