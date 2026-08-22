import re
import unicodedata


SINHALA_START = "\u0D80"
SINHALA_END = "\u0DFF"
HARD_TERMINATORS = frozenset("!?।෴")
CLOSING_PUNCTUATION = frozenset('"\'”’»)]}')
TOKEN_PATTERN = re.compile(r"[\u0D80-\u0DFFA-Za-z0-9]+")
REMOVABLE_FORMATTING = re.compile(r"[\u00AD\u200B\u2060\uFEFF]")
HORIZONTAL_WHITESPACE = re.compile(r"[^\S\r\n]+")

ABBREVIATIONS = frozenset(
    {
        "රු.",
        "පෙ.ව.",
        "ප.ව.",
        "පූ.ව.",
        "කි.මී.",
        "සෙ.මී.",
        "ව.කි.මී.",
        "මි.මී.",
        "මි.",
        "මී.",
        "කි.ග්‍රෑ.",
        "ග්‍රෑ.",
        "mr.",
        "mrs.",
        "ms.",
        "dr.",
        "prof.",
        "no.",
        "vs.",
        "etc.",
        "a.m.",
        "p.m.",
    }
)
NAME_INITIAL_ABBREVIATIONS = frozenset(
    {
        "ඒ.",
        "බී.",
        "සී.",
        "ඩී.",
        "ඊ.",
        "එෆ්.",
        "ජී.",
        "එච්.",
        "අයි.",
        "ජේ.",
        "කේ.",
        "එල්.",
        "එම්.",
        "එන්.",
        "ඕ.",
        "පී.",
        "කිව්.",
        "ආර්.",
        "එස්.",
        "ටී.",
        "යූ.",
        "වී.",
        "ඩබ්ලිව්.",
        "එක්ස්.",
        "වයි.",
        "සෙඩ්.",
    }
)


def normalize_sentence(text: str) -> str:
    normalized = unicodedata.normalize("NFC", text or "")
    normalized = REMOVABLE_FORMATTING.sub("", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def split_sentences(text: str) -> list[str]:
    normalized = _normalize_text_preserving_lines(text)
    sentences = []
    for line in normalized.split("\n"):
        line = line.strip()
        if line:
            sentences.extend(_split_line(line))
    return sentences


def extract_usable_sentences(
    text: str,
    min_chars: int = 20,
    min_words: int = 3,
    require_sinhala: bool = True,
) -> list[str]:
    if min_chars < 1:
        raise ValueError("min_chars must be at least 1")
    if min_words < 1:
        raise ValueError("min_words must be at least 1")

    return [
        sentence
        for sentence in split_sentences(text)
        if is_usable_sentence(
            sentence,
            min_chars=min_chars,
            min_words=min_words,
            require_sinhala=require_sinhala,
        )
    ]


def is_usable_sentence(
    sentence: str,
    min_chars: int = 20,
    min_words: int = 3,
    require_sinhala: bool = True,
) -> bool:
    normalized = normalize_sentence(sentence)
    content_length = sum(not char.isspace() for char in normalized)
    words = TOKEN_PATTERN.findall(normalized)
    has_sinhala = any(SINHALA_START <= char <= SINHALA_END for char in normalized)
    return (
        content_length >= min_chars
        and len(words) >= min_words
        and (has_sinhala or not require_sinhala)
    )


def _normalize_text_preserving_lines(text: str) -> str:
    normalized = unicodedata.normalize("NFC", text or "")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    normalized = REMOVABLE_FORMATTING.sub("", normalized)
    normalized = HORIZONTAL_WHITESPACE.sub(" ", normalized)
    normalized = re.sub(r" *\n *", "\n", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)
    return normalized.strip()


def _split_line(line: str) -> list[str]:
    sentences = []
    start = 0
    index = 0
    while index < len(line):
        char = line[index]
        if char not in HARD_TERMINATORS and char != ".":
            index += 1
            continue

        if char == "." and _is_protected_period(line, index):
            index += 1
            continue

        boundary_end = _boundary_end(line, index)
        if (
            boundary_end < len(line)
            and not line[boundary_end].isspace()
            and not _can_start_without_space(char, line[boundary_end])
        ):
            index = boundary_end
            continue

        sentence = normalize_sentence(line[start:boundary_end])
        if sentence:
            sentences.append(sentence)
        start = boundary_end
        while start < len(line) and line[start].isspace():
            start += 1
        index = start

    remainder = normalize_sentence(line[start:])
    if remainder:
        sentences.append(remainder)
    return sentences


def _boundary_end(text: str, index: int) -> int:
    end = index + 1
    while end < len(text) and (
        text[end] in HARD_TERMINATORS or text[end] == "."
    ):
        end += 1
    while end < len(text) and text[end] in CLOSING_PUNCTUATION:
        end += 1
    return end


def _is_protected_period(text: str, index: int) -> bool:
    previous_char = text[index - 1] if index > 0 else ""
    next_char = text[index + 1] if index + 1 < len(text) else ""
    if previous_char.isdigit() and next_char.isdigit():
        return True

    token_start = index
    while token_start > 0 and not text[token_start - 1].isspace():
        token_start -= 1
    token = text[token_start : index + 1].lstrip('"\'“‘([{').casefold()
    if token in ABBREVIATIONS:
        return True
    if token in NAME_INITIAL_ABBREVIATIONS and _has_following_content(
        text,
        index,
    ):
        return True

    stem = token[:-1]
    if len(stem) == 1 and stem.isalpha():
        return True
    if stem.isdigit() and token_start == 0:
        return True

    acronym_parts = [part for part in stem.split(".") if part]
    return (
        len(acronym_parts) >= 2
        and all(_is_initial_part(part) for part in acronym_parts)
    )


def _has_following_content(text: str, index: int) -> bool:
    return bool(text[index + 1 :].strip())


def _is_initial_part(part: str) -> bool:
    return (
        f"{part}." in NAME_INITIAL_ABBREVIATIONS
        or (len(part) == 1 and part.isalpha())
    )


def _can_start_without_space(terminator: str, next_char: str) -> bool:
    if terminator in HARD_TERMINATORS:
        return True
    return (
        SINHALA_START <= next_char <= SINHALA_END
        or next_char.isupper()
        or next_char in '"\'“‘([{'
    )
