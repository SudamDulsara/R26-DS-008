import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional


TOKEN_PATTERN = re.compile(r"[\u0D80-\u0DFFA-Za-z0-9]+")


@dataclass(frozen=True)
class ClusterArticle:
    id: int
    source: str
    title: str
    published_date: str
    crawl_timestamp: str
    clean_text: str
    event_time: Optional[datetime]
    similarity_text: str


def normalize_for_similarity(text: str) -> str:
    text = unicodedata.normalize("NFC", text or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize_for_similarity(text: str) -> set[str]:
    normalized = normalize_for_similarity(text).lower()
    return {match.group(0) for match in TOKEN_PATTERN.finditer(normalized)}


def lexical_overlap(left_text: str, right_text: str) -> float:
    left_tokens = tokenize_for_similarity(left_text)
    right_tokens = tokenize_for_similarity(right_text)

    if not left_tokens or not right_tokens:
        return 0.0

    return len(left_tokens & right_tokens) / min(len(left_tokens), len(right_tokens))


def first_paragraph(text: str, char_limit: int) -> str:
    for paragraph in re.split(r"\n\s*\n", text or ""):
        paragraph = normalize_for_similarity(paragraph)
        if paragraph:
            return paragraph[:char_limit].strip()
    return normalize_for_similarity(text)[:char_limit].strip()


def build_similarity_text(title: str, clean_text: str, lead_char_limit: int) -> str:
    parts = [
        normalize_for_similarity(title),
        first_paragraph(clean_text, lead_char_limit),
    ]
    return "\n".join(part for part in parts if part)


def parse_article_datetime(*values: str) -> Optional[datetime]:
    for value in values:
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        return _as_naive_utc(datetime.fromisoformat(normalized))
    except ValueError:
        pass

    try:
        return _as_naive_utc(parsedate_to_datetime(text))
    except (TypeError, ValueError):
        pass

    for date_format in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ):
        try:
            return _as_naive_utc(datetime.strptime(text, date_format))
        except ValueError:
            continue

    return None


def _as_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)
