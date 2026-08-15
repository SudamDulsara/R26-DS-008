import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Optional
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class MetadataCandidate:
    value: str
    source: str


@dataclass(frozen=True)
class ArticleMetadata:
    title: str
    title_source: str
    author: str
    author_source: str
    published_date: str
    published_date_source: str
    category: str
    category_source: str
    metadata_flags: list[str]


def compute_text_hash(text: str) -> str:
    text = text or ""
    if not text.strip():
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def extract_category_from_url(url: str, source: str) -> str:
    parts = [part for part in urlparse(url).path.split("/") if part]

    if source == "Dinamina" and len(parts) >= 4:
        return parts[3]
    if source in {"NethnewsLk", "Divaina"} and len(parts) >= 1:
        return parts[0]
    return ""


def extract_metadata(
    url: str,
    source: str,
    trafilatura_data: dict[str, Any],
    html: str,
    rss_title: str = "",
    rss_published: str = "",
) -> ArticleMetadata:
    soup = _make_soup(html)
    json_ld_items = _extract_json_ld_items(soup, html)

    title = _first_candidate(
        _from_value(trafilatura_data.get("title"), "trafilatura"),
        _from_value(rss_title, "rss"),
        _html_meta(soup, html, ["og:title", "twitter:title"], "html_meta"),
        _json_ld_value(json_ld_items, ["headline", "name"], "json_ld"),
        _heading_value(soup, html, "h1"),
        _html_title(soup, html),
        _url_slug_title(url),
    )

    author = _first_candidate(
        _from_value(trafilatura_data.get("author"), "trafilatura"),
        _json_ld_author(json_ld_items),
        _html_meta_name(soup, html, ["author", "article:author"], "html_meta"),
    )

    raw_date = _first_candidate(
        _from_value(trafilatura_data.get("date"), "trafilatura"),
        _from_value(rss_published, "rss"),
        _json_ld_value(
            json_ld_items,
            ["datePublished", "dateCreated", "dateModified"],
            "json_ld",
        ),
        _html_meta(
            soup,
            html,
            [
                "article:published_time",
                "article:modified_time",
                "og:updated_time",
                "datePublished",
                "date",
                "pubdate",
            ],
            "html_meta",
        ),
        _time_value(soup, html),
        _url_date(url),
    )

    category = _first_candidate(
        _json_ld_value(json_ld_items, ["articleSection", "section"], "json_ld"),
        _html_meta(
            soup,
            html,
            ["article:section", "section", "category"],
            "html_meta",
        ),
        _from_value(extract_category_from_url(url, source), "url"),
    )

    normalized_date = _normalize_date(raw_date.value) if raw_date else ""
    date_source = raw_date.source if raw_date and normalized_date else ""

    flags = []
    if not title:
        flags.append("missing_title")
    elif title.source == "url_slug":
        flags.append("title_from_url_slug")
    if raw_date and not normalized_date:
        flags.append("unparsed_published_date")
    if not normalized_date:
        flags.append("missing_published_date")

    return ArticleMetadata(
        title=_clean_title(title.value) if title else "",
        title_source=title.source if title else "",
        author=author.value if author else "",
        author_source=author.source if author else "",
        published_date=normalized_date,
        published_date_source=date_source,
        category=category.value if category else "",
        category_source=category.source if category else "",
        metadata_flags=flags,
    )


def final_metadata_flags(
    title: str,
    published_date: str,
    content_hash: str,
    existing_flags: Optional[list[str]] = None,
) -> list[str]:
    flags = [
        flag
        for flag in (existing_flags or [])
        if flag not in {"missing_title", "missing_published_date", "missing_content_hash"}
    ]
    if not (title or "").strip():
        flags.append("missing_title")
    if not (published_date or "").strip():
        flags.append("missing_published_date")
    if not (content_hash or "").strip():
        flags.append("missing_content_hash")
    return sorted(set(flags))


def _make_soup(html: str):
    if not html:
        return None
    try:
        from bs4 import BeautifulSoup
    except ModuleNotFoundError:
        return None
    return BeautifulSoup(html, "html.parser")


def _from_value(value: Any, source: str) -> Optional[MetadataCandidate]:
    text = _clean_text(value)
    if not text:
        return None
    return MetadataCandidate(text, source)


def _first_candidate(*candidates: Optional[MetadataCandidate]):
    for candidate in candidates:
        if candidate and candidate.value:
            return candidate
    return None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = ", ".join(str(item) for item in value if item)
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text


def _clean_title(title: str) -> str:
    title = _clean_text(title)
    for separator in (" | ", " - ", " – ", " — "):
        parts = [part.strip() for part in title.split(separator) if part.strip()]
        if len(parts) > 1:
            return parts[0]
    return title


def _html_meta(
    soup,
    html: str,
    keys: list[str],
    source: str,
) -> Optional[MetadataCandidate]:
    if soup is None:
        return _regex_meta(html, keys, source)

    for key in keys:
        selectors = [
            {"property": key},
            {"name": key},
            {"itemprop": key},
        ]
        for attrs in selectors:
            tag = soup.find("meta", attrs=attrs)
            if tag and tag.get("content"):
                return _from_value(tag.get("content"), source)
    return None


def _html_meta_name(
    soup,
    html: str,
    keys: list[str],
    source: str,
) -> Optional[MetadataCandidate]:
    return _html_meta(soup, html, keys, source)


def _html_title(soup, html: str) -> Optional[MetadataCandidate]:
    if soup is None or soup.title is None:
        return _regex_tag_text(html, "title", "html_title")
    return _from_value(soup.title.get_text(" ", strip=True), "html_title")


def _heading_value(soup, html: str, tag_name: str) -> Optional[MetadataCandidate]:
    if soup is None:
        return _regex_tag_text(html, tag_name, tag_name)
    tag = soup.find(tag_name)
    if tag is None:
        return None
    return _from_value(tag.get_text(" ", strip=True), tag_name)


def _time_value(soup, html: str) -> Optional[MetadataCandidate]:
    if soup is None:
        return _regex_time_value(html)
    for tag in soup.find_all("time"):
        value = tag.get("datetime") or tag.get_text(" ", strip=True)
        candidate = _from_value(value, "time_tag")
        if candidate:
            return candidate
    return None


def _extract_json_ld_items(soup, html: str) -> list[Any]:
    if soup is None:
        return _regex_json_ld_items(html)

    items = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw_json = script.string or script.get_text()
        if not raw_json:
            continue
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        items.extend(_flatten_json_ld(parsed))
    return items


def _regex_meta(
    html: str,
    keys: list[str],
    source: str,
) -> Optional[MetadataCandidate]:
    key_set = {key.lower() for key in keys}
    for tag in re.findall(r"<meta\b[^>]*>", html or "", flags=re.IGNORECASE):
        attrs = _regex_attrs(tag)
        content = attrs.get("content", "")
        if not content:
            continue
        for attr_name in ("property", "name", "itemprop"):
            if attrs.get(attr_name, "").lower() in key_set:
                return _from_value(content, source)
    return None


def _regex_attrs(tag: str) -> dict[str, str]:
    attrs = {}
    pattern = r"""([:\w-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))"""
    for match in re.finditer(pattern, tag or ""):
        value = match.group(2) or match.group(3) or match.group(4) or ""
        attrs[match.group(1).lower()] = unescape(value)
    return attrs


def _regex_tag_text(
    html: str,
    tag_name: str,
    source: str,
) -> Optional[MetadataCandidate]:
    pattern = rf"<{re.escape(tag_name)}\b[^>]*>(.*?)</{re.escape(tag_name)}>"
    match = re.search(pattern, html or "", flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    text = re.sub(r"<[^>]+>", " ", match.group(1))
    return _from_value(unescape(text), source)


def _regex_time_value(html: str) -> Optional[MetadataCandidate]:
    for tag in re.findall(
        r"<time\b[^>]*>.*?</time>",
        html or "",
        flags=re.IGNORECASE | re.DOTALL,
    ):
        attrs = _regex_attrs(tag)
        if attrs.get("datetime"):
            return _from_value(attrs["datetime"], "time_tag")
        text = re.sub(r"<[^>]+>", " ", tag)
        candidate = _from_value(unescape(text), "time_tag")
        if candidate:
            return candidate
    return None


def _regex_json_ld_items(html: str) -> list[Any]:
    items = []
    pattern = (
        r"<script\b[^>]*type=[\"']application/ld\+json[\"'][^>]*>"
        r"(.*?)</script>"
    )
    for raw_json in re.findall(pattern, html or "", flags=re.IGNORECASE | re.DOTALL):
        try:
            parsed = json.loads(unescape(raw_json).strip())
        except json.JSONDecodeError:
            continue
        items.extend(_flatten_json_ld(parsed))
    return items


def _flatten_json_ld(value: Any) -> list[Any]:
    if isinstance(value, list):
        items = []
        for item in value:
            items.extend(_flatten_json_ld(item))
        return items
    if isinstance(value, dict):
        items = [value]
        graph = value.get("@graph")
        if isinstance(graph, list):
            items.extend(_flatten_json_ld(graph))
        return items
    return []


def _json_ld_value(
    items: list[Any],
    keys: list[str],
    source: str,
) -> Optional[MetadataCandidate]:
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in keys:
            candidate = _from_value(item.get(key), source)
            if candidate:
                return candidate
    return None


def _json_ld_author(items: list[Any]) -> Optional[MetadataCandidate]:
    for item in items:
        if not isinstance(item, dict):
            continue
        author = item.get("author")
        if isinstance(author, dict):
            candidate = _from_value(author.get("name"), "json_ld")
        elif isinstance(author, list):
            names = [
                value.get("name") if isinstance(value, dict) else value
                for value in author
            ]
            candidate = _from_value(names, "json_ld")
        else:
            candidate = _from_value(author, "json_ld")
        if candidate:
            return candidate
    return None


def _url_slug_title(url: str) -> Optional[MetadataCandidate]:
    path_parts = [part for part in urlparse(url).path.split("/") if part]
    if not path_parts:
        return None

    slug = unquote(path_parts[-1]).strip("/")
    if not slug or re.fullmatch(r"\d+", slug):
        return None

    title = re.sub(r"[-_]+", " ", slug).strip()
    if len(title) < 5:
        return None
    return MetadataCandidate(title, "url_slug")


def _url_date(url: str) -> Optional[MetadataCandidate]:
    match = re.search(r"/((?:19|20)\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)", url)
    if not match:
        return None

    year, month, day = match.groups()
    return MetadataCandidate(f"{year}-{int(month):02d}-{int(day):02d}", "url_date")


def _normalize_date(value: str) -> str:
    if not value:
        return ""

    text = _clean_text(value).replace("Z", "+00:00")
    parsed = None

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        pass

    if parsed is None:
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            pass

    if parsed is None:
        try:
            import dateparser

            parsed = dateparser.parse(text)
        except ModuleNotFoundError:
            parsed = None

    if parsed is None:
        return ""

    return parsed.isoformat(timespec="seconds")
