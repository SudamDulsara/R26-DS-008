from __future__ import annotations

import re
import unicodedata


SINHALA_CHARACTER_RANGE = "\u0D80-\u0DFF"
NUMBER_PATTERN = re.compile(
    r"(?:[0-9\u0DE6-\u0DEF]+(?:[,.][0-9\u0DE6-\u0DEF]+)*"
    r"|\.[0-9\u0DE6-\u0DEF]+)"
)
ISO_DATE_PATTERN = re.compile(
    r"(?<!\d)(?:19|20)\d{2}[-/.](0?[1-9]|1[0-2])[-/.]"
    r"(?:0?[1-9]|[12]\d|3[01])(?!\d)"
)
TIME_PATTERN = re.compile(
    r"(?<![0-9\u0DE6-\u0DEF])"
    r"([0-9\u0DE6-\u0DEF]{1,2})([:.])"
    r"([0-9\u0DE6-\u0DEF]{2})"
    r"(?![0-9\u0DE6-\u0DEF])"
)
SINHALA_DECIMAL_PATTERN = re.compile(
    r"(?<![0-9\u0DE6-\u0DEF])"
    r"([0-9\u0DE6-\u0DEF]+)\s*යි\s+දශම\s+"
    r"([0-9\u0DE6-\u0DEF]+"
    r"(?:\s*යි\s*[0-9\u0DE6-\u0DEF]+)*)"
)


REVIEWED_SINHALA_NUMBER_WORDS = (
    ("හයලක්ෂ විසිපන්දහස", 625_000),
    ("හැත්තෑ හත් දහස", 77_000),
    ("හැත්තෑ හත්දහස", 77_000),
    ("තිස් හය දහස", 36_000),
    ("තිස් හයදහස", 36_000),
    ("සියයට සියය", 100),
    ("නවසිය අනූ", 990),
    ("හැට නව", 69),
    ("හතළිහ", 40),
    ("හතලිහ", 40),
    ("පනහ", 50),
    ("අසූ", 80),
    ("දහස", 1_000),
    ("දහය", 10),
    ("දස", 10),
)

_NUMBER_WORD_SUFFIX = (
    r"(?:ක්ම?|ක(?:ට|ින්|ගේ)?|ව(?:ක්|ෙහි)?|"
    r"දෙන(?:ා|ෙක්|ෙකු|කු)?)?"
)


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFC", str(value)).casefold()
    normalized = normalized.replace("\u200c", "").replace("\u200d", "")
    normalized = normalized.replace("\u2018", "'").replace("\u2019", "'")
    return re.sub(r"\s+", " ", normalized).strip()


def normalize_number(value: str) -> str:
    digits = []
    for character in value:
        if character == ",":
            continue
        if character == ".":
            digits.append(character)
            continue
        try:
            digits.append(str(unicodedata.digit(character)))
        except (TypeError, ValueError):
            digits.append(character)
    normalized = "".join(digits)
    if "." in normalized:
        whole, fraction = normalized.split(".", 1)
        return (
            f"{whole.lstrip('0') or '0'}."
            f"{fraction.rstrip('0') or '0'}"
        )
    return normalized.lstrip("0") or "0"


def numeric_literals(value: str) -> set[str]:
    numbers = {
        normalize_number(match.group(0))
        for match in NUMBER_PATTERN.finditer(value)
    }
    for match in ISO_DATE_PATTERN.finditer(value):
        numbers.update(
            normalize_number(component)
            for component in re.split(r"[-/.]", match.group(0))
        )
    return numbers


def time_values(value: str) -> dict[str, str]:
    """Map each numeric rendering to an HH:MM identity.

    Sri Lankan news sources commonly render times with a colon while generated
    Sinhala prose uses a dot. The raw rendering remains available as the key so
    only the matching generated numeric token is waived.
    """
    values: dict[str, str] = {}
    for match in TIME_PATTERN.finditer(value):
        hour = int(normalize_number(match.group(1)))
        minute = int(normalize_number(match.group(3)))
        if hour > 23 or minute > 59:
            continue
        raw = normalize_number(
            f"{match.group(1)}.{match.group(3)}"
        )
        values[raw] = f"{hour:02d}:{minute:02d}"
    return values


def sinhala_number_word_values(value: str) -> set[str]:
    normalized = normalize_text(value)
    values: set[str] = set()
    for phrase, number in REVIEWED_SINHALA_NUMBER_WORDS:
        pattern = re.compile(
            rf"(?<![{SINHALA_CHARACTER_RANGE}])"
            rf"{re.escape(phrase)}{_NUMBER_WORD_SUFFIX}"
            rf"(?![{SINHALA_CHARACTER_RANGE}])"
        )
        if pattern.search(normalized):
            values.add(str(number))
    return values


def sinhala_decimal_values(value: str) -> set[str]:
    """Extract source forms such as ``81යි දශම 29`` as ``81.29``."""
    values = set()
    for match in SINHALA_DECIMAL_PATTERN.finditer(value):
        whole = normalize_number(match.group(1))
        fraction = re.sub(r"\s*යි\s*", "", match.group(2))
        fraction = "".join(
            str(unicodedata.digit(character))
            for character in fraction
        )
        values.add(normalize_number(f"{whole}.{fraction}"))
    return values


def sinhala_entity_aliases(value: str) -> set[str]:
    """Return conservative grammatical aliases for one extracted name token."""
    normalized = normalize_text(value)
    aliases = {normalized}
    for suffix in ("ගේ", "ගෙන්", "ට", "ව"):
        if normalized.endswith(suffix):
            base = normalized[: -len(suffix)]
            if len(base) >= 3:
                aliases.add(base)
    return aliases
