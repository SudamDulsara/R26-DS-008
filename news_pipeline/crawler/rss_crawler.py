from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from news_pipeline.clustering.text import parse_article_datetime
from news_pipeline.config import load_config
from news_pipeline.statuses import URL_STATUS_DISCOVERED
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


sqlite3.register_adapter(datetime, lambda value: value.isoformat())

logger = get_logger()
ADA_DERANA_SOURCE = "Ada Derana Sinhala"
ADA_DERANA_ARCHIVE_URL = "https://sinhala.adaderana.lk/news_archive.php?srcRslt=1"
INITIAL_DISCOVERY_LOOKBACK_HOURS = 24


def build_feed_page_urls(rss_url: str, feed_pages: int) -> list[str]:
    if feed_pages < 1:
        raise ValueError("feed_pages must be at least 1")
    urls = [rss_url]
    path = urlsplit(rss_url).path.rstrip("/").lower()
    if feed_pages == 1 or not path.endswith("/feed"):
        return urls
    split = urlsplit(rss_url)
    base_query = dict(parse_qsl(split.query, keep_blank_values=True))
    for page in range(2, feed_pages + 1):
        query = {**base_query, "paged": str(page)}
        urls.append(
            urlunsplit(
                (
                    split.scheme,
                    split.netloc,
                    split.path,
                    urlencode(query),
                    split.fragment,
                )
            )
        )
    return urls


def _entry_value(entry, *names: str) -> str:
    for name in names:
        value = entry.get(name, "")
        if value:
            return str(value).strip()
    return ""


def _normalized_feed_entry(entry) -> dict[str, str]:
    url = _entry_value(entry, "link")
    if not url and entry.get("links"):
        url = str(entry.links[0].get("href", "")).strip()
    summary = _entry_value(entry, "summary", "description")
    return {
        "url": url,
        "title": _entry_value(entry, "title"),
        "published": _entry_value(entry, "published", "updated"),
        "summary": summary,
    }


def _feed_page_signature(entries: list[dict[str, str]]) -> tuple[str, ...]:
    """Identify publisher pagination loops without depending on entry order."""
    return tuple(
        sorted(
            str(entry.get("url") or "").strip()
            for entry in entries
            if str(entry.get("url") or "").strip()
        )
    )


def _insert_entries(
    *,
    connection,
    source: str,
    entries: list[dict[str, str]],
    discovered_at: str,
    discovery_method: str,
) -> dict[str, int]:
    cursor = connection.cursor()
    counts = {"entries": len(entries), "new": 0, "known": 0, "invalid": 0}
    for entry in entries:
        url = str(entry.get("url") or "").strip()
        if not url:
            counts["invalid"] += 1
            continue
        existed = cursor.execute(
            "SELECT 1 FROM discovered_urls WHERE url = ?",
            (url,),
        ).fetchone() is not None
        cursor.execute(
            """
            INSERT INTO discovered_urls (
                url, source, discovered_at, rss_title, rss_published,
                rss_summary, discovery_method, status, fetched
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(url) DO UPDATE SET
                rss_title = COALESCE(discovered_urls.rss_title, excluded.rss_title),
                rss_published = COALESCE(
                    discovered_urls.rss_published,
                    excluded.rss_published
                ),
                rss_summary = COALESCE(
                    discovered_urls.rss_summary,
                    excluded.rss_summary
                )
            """,
            (
                url,
                source,
                discovered_at,
                entry.get("title") or None,
                entry.get("published") or None,
                entry.get("summary") or None,
                discovery_method,
                URL_STATUS_DISCOVERED,
            ),
        )
        counts["known" if existed else "new"] += 1
    return counts


def _previous_checkpoint(connection, source: str) -> datetime | None:
    row = connection.execute(
        """
        SELECT covered_through
        FROM source_discovery_checkpoints
        WHERE source = ?
        """,
        (source,),
    ).fetchone()
    if row is not None:
        return parse_article_datetime(str(row["covered_through"] or ""))

    rows = connection.execute(
        """
        SELECT started_at
        FROM pipeline_runs
        WHERE status = 'completed'
        ORDER BY COALESCE(finished_at, started_at) DESC, id DESC
        LIMIT 2
        """
    ).fetchall()
    if not rows:
        return None
    # The checkpoint table was introduced after existing installations had
    # already run the fixed-size RSS crawler. Replay one extra completed-run
    # interval during migration so a shallow final feed from the last legacy
    # run cannot silently become the new boundary.
    migration_row = rows[1] if len(rows) > 1 else rows[0]
    return parse_article_datetime(str(migration_row["started_at"] or ""))


def _oldest_entry_time(entries: list[dict[str, str]]) -> datetime | None:
    values = [
        parsed
        for entry in entries
        if (parsed := parse_article_datetime(entry.get("published", "")))
        is not None
    ]
    return min(values) if values else None


def _entries_reach_boundary(
    entries: list[dict[str, str]],
    boundary: datetime,
    connection,
) -> bool:
    oldest = _oldest_entry_time(entries)
    if oldest is not None:
        return oldest <= boundary
    urls = [entry["url"] for entry in entries if entry.get("url")]
    if not urls:
        return False
    placeholders = ",".join("?" for _ in urls)
    row = connection.execute(
        f"""
        SELECT 1
        FROM discovered_urls
        WHERE url IN ({placeholders})
          AND discovered_at <= ?
        LIMIT 1
        """,
        (*urls, boundary.isoformat()),
    ).fetchone()
    return row is not None


def _fetch_rss_page(url: str, label: str) -> tuple[list[dict[str, str]], str | None]:
    try:
        import feedparser
    except ModuleNotFoundError as exc:
        return [], f"missing dependency: {exc.name}"
    logger.info("[%s] Fetching RSS feed...", label)
    feed = feedparser.parse(url)
    if feed.bozo:
        logger.warning("[%s] Feed may have issues - %s", label, feed.bozo_exception)
        if not feed.entries:
            return [], "feed_parse_error"
    return [_normalized_feed_entry(entry) for entry in feed.entries], None


def _ada_archive_entries(day: datetime) -> tuple[list[dict[str, str]], str | None]:
    try:
        import requests
        from bs4 import BeautifulSoup
    except ModuleNotFoundError as exc:
        return [], f"missing dependency: {exc.name}"
    try:
        response = requests.post(
            ADA_DERANA_ARCHIVE_URL,
            data={
                "srcCategory": "999",
                "srcYear": str(day.year),
                "srcMonth": f"{day.month:02d}",
                "srcDay": str(day.day),
                "Submit": "Search",
            },
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 news-pipeline/1.0"},
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        return [], f"archive_request_error:{type(exc).__name__}"

    soup = BeautifulSoup(response.text, "html.parser")
    entries: list[dict[str, str]] = []
    for story in soup.select(".news-story"):
        link = story.select_one("h2 a[href]")
        if link is None:
            continue
        published_node = story.select_one(".comments span")
        summary_node = story.select_one(".story-text > p")
        entries.append(
            {
                # Match the publisher's RSS URL form so the SQLite URL key
                # remains idempotent even though the archive itself is HTTPS.
                "url": urljoin(
                    "http://sinhala.adaderana.lk/",
                    link.get("href", ""),
                ),
                "title": link.get_text(" ", strip=True),
                "published": (
                    published_node.get_text(" ", strip=True).lstrip("| ")
                    if published_node is not None
                    else ""
                ),
                "summary": (
                    summary_node.get_text(" ", strip=True)
                    if summary_node is not None
                    else ""
                ),
            }
        )
    if not entries:
        return [], "archive_empty"
    return entries, None


def _ada_catchup(
    *,
    connection,
    boundary: datetime,
    run_started: datetime,
    max_days: int,
) -> tuple[dict[str, int], str | None, int]:
    local_zone = datetime.now().astimezone().tzinfo or timezone.utc
    boundary_day = boundary.replace(tzinfo=timezone.utc).astimezone(local_zone).date()
    run_day = run_started.replace(tzinfo=timezone.utc).astimezone(local_zone).date()
    day_count = (run_day - boundary_day).days + 1
    if day_count > max_days:
        return {"entries": 0, "new": 0, "known": 0, "invalid": 0}, "catchup_day_limit", 0

    totals = {"entries": 0, "new": 0, "known": 0, "invalid": 0}
    requests_made = 0
    for offset in range(day_count):
        day = datetime.combine(boundary_day + timedelta(days=offset), datetime.min.time())
        entries, error = _ada_archive_entries(day)
        requests_made += 1
        if error == "archive_empty" and day.date() == run_day:
            # The publisher often creates the current-day archive page only
            # after its first item. An empty current day does not invalidate a
            # successfully completed catch-up through yesterday.
            continue
        if error is not None:
            return totals, error, requests_made
        counts = _insert_entries(
            connection=connection,
            source=ADA_DERANA_SOURCE,
            entries=entries,
            discovered_at=run_started.isoformat(),
            discovery_method="archive_checkpoint_catchup",
        )
        for key in totals:
            totals[key] += counts[key]
    return totals, None, requests_made


def _save_checkpoint(
    *,
    connection,
    source: str,
    run_started: datetime,
    status: str,
    boundary_url: str | None,
    details: dict,
) -> None:
    connection.execute(
        """
        INSERT INTO source_discovery_checkpoints (
            source, covered_through, last_success_at, coverage_status,
            boundary_url, details_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source) DO UPDATE SET
            covered_through = excluded.covered_through,
            last_success_at = excluded.last_success_at,
            coverage_status = excluded.coverage_status,
            boundary_url = excluded.boundary_url,
            details_json = excluded.details_json
        """,
        (
            source,
            run_started.isoformat(),
            datetime.now().astimezone().isoformat(timespec="seconds"),
            status,
            boundary_url,
            json.dumps(details, ensure_ascii=False, sort_keys=True),
        ),
    )


def discover_urls(
    source_name,
    rss_url,
    connection,
    log_label=None,
    telemetry=None,
):
    """Compatibility entry point for a single RSS page."""
    label = log_label or source_name
    entries, error = _fetch_rss_page(rss_url, label)
    if error is not None:
        if telemetry is not None:
            source_failures = telemetry.setdefault(source_name, {})
            source_failures[error] = source_failures.get(error, 0) + 1
        return 0
    counts = _insert_entries(
        connection=connection,
        source=source_name,
        entries=entries,
        discovered_at=datetime.now().isoformat(timespec="seconds"),
        discovery_method="rss",
    )
    logger.info("[%s] Done - %s new URLs saved.", label, counts["new"])
    return counts["new"]


def _discover_source(
    *,
    source: str,
    rss_url: str,
    connection,
    run_started: datetime,
    overlap_hours: int,
    max_pages: int,
    max_catchup_days: int,
) -> dict:
    checkpoint = _previous_checkpoint(connection, source)
    boundary = (
        checkpoint - timedelta(hours=overlap_hours)
        if checkpoint is not None
        else run_started - timedelta(hours=INITIAL_DISCOVERY_LOOKBACK_HOURS)
    )
    is_wordpress = urlsplit(rss_url).path.rstrip("/").lower().endswith("/feed")
    page_urls = build_feed_page_urls(rss_url, max_pages if is_wordpress else 1)
    totals = {"entries": 0, "new": 0, "known": 0, "invalid": 0}
    pages_fetched = 0
    boundary_reached = False
    boundary_url = None
    error = None
    stop_reason = None
    page_signatures: set[tuple[str, ...]] = set()

    for page_number, page_url in enumerate(page_urls, start=1):
        label = source if len(page_urls) == 1 else f"{source} feed page {page_number}"
        entries, error = _fetch_rss_page(page_url, label)
        pages_fetched += 1
        if error is not None:
            stop_reason = error
            break
        if page_number > 1 and not entries:
            boundary_reached = True
            stop_reason = "feed_exhausted_empty_page"
            break
        if not entries:
            stop_reason = "feed_empty"
            if source != ADA_DERANA_SOURCE:
                error = stop_reason
            break
        page_signature = _feed_page_signature(entries)
        if page_number > 1 and page_signature in page_signatures:
            boundary_reached = True
            stop_reason = "feed_pagination_repeated_page"
            break
        page_signatures.add(page_signature)
        counts = _insert_entries(
            connection=connection,
            source=source,
            entries=entries,
            discovered_at=run_started.isoformat(),
            discovery_method="rss" if page_number == 1 else "rss_checkpoint_catchup",
        )
        for key in totals:
            totals[key] += counts[key]
        oldest = _oldest_entry_time(entries)
        if oldest is not None:
            boundary_url = min(
                (
                    entry
                    for entry in entries
                    if parse_article_datetime(entry.get("published", "")) is not None
                ),
                key=lambda item: parse_article_datetime(item.get("published", "")),
            ).get("url")
        if _entries_reach_boundary(entries, boundary, connection):
            boundary_reached = True
            stop_reason = "checkpoint_boundary_reached"
            break
        if not is_wordpress:
            stop_reason = "single_page_feed"
            break

    if (
        error is None
        and not boundary_reached
        and is_wordpress
        and pages_fetched >= len(page_urls)
    ):
        stop_reason = "max_pages_reached_before_checkpoint"

    archive_requests = 0
    used_archive = False
    if (
        error is None
        and not boundary_reached
        and source == ADA_DERANA_SOURCE
    ):
        used_archive = True
        archive_counts, error, archive_requests = _ada_catchup(
            connection=connection,
            boundary=boundary,
            run_started=run_started,
            max_days=max_catchup_days,
        )
        for key in totals:
            totals[key] += archive_counts[key]
        boundary_reached = error is None
        stop_reason = (
            "archive_checkpoint_catchup"
            if boundary_reached
            else error
        )

    status = (
        "coverage_complete_with_catchup"
        if boundary_reached and (pages_fetched > 1 or used_archive)
        else "coverage_complete"
        if boundary_reached
        else "source_unavailable"
        if error is not None
        else "coverage_uncertain"
    )
    details = {
        **totals,
        "checkpoint": checkpoint.isoformat() if checkpoint else None,
        "effective_boundary": boundary.isoformat() if boundary else None,
        "feed_pages_fetched": pages_fetched,
        "archive_requests": archive_requests,
        "stop_reason": stop_reason,
        "error": error,
    }
    if boundary_reached:
        _save_checkpoint(
            connection=connection,
            source=source,
            run_started=run_started,
            status=status,
            boundary_url=boundary_url,
            details=details,
        )
    logger.info(
        "[%s] Done - %s new URLs | %s (%s entries, %s known)",
        source,
        totals["new"],
        status,
        totals["entries"],
        totals["known"],
    )
    return {"status": status, **details}


def run_discovery(feed_pages: int | None = None):
    config = load_config()
    connection = get_connection()
    run_started = datetime.now(timezone.utc).replace(tzinfo=None)
    max_pages = feed_pages or config.discovery_max_pages
    if max_pages < 1:
        raise ValueError("feed_pages must be at least 1")

    logger.info("=== URL Discovery Started ===")
    total = 0
    failures_by_source = {}
    coverage_by_source = {}
    try:
        for source_name, rss_url in config.news_sources.items():
            result = _discover_source(
                source=source_name,
                rss_url=rss_url,
                connection=connection,
                run_started=run_started,
                overlap_hours=config.discovery_overlap_hours,
                max_pages=max_pages,
                max_catchup_days=config.discovery_max_catchup_days,
            )
            total += int(result["new"])
            coverage_by_source[source_name] = result
            if result.get("error"):
                failures_by_source[source_name] = {str(result["error"]): 1}
            logger.info("")
        connection.commit()
    finally:
        connection.close()

    coverage_counts: dict[str, int] = {}
    for result in coverage_by_source.values():
        status = str(result["status"])
        coverage_counts[status] = coverage_counts.get(status, 0) + 1
    logger.info("=== Discovery Complete - %s total new URLs found ===", total)
    return {
        "new_urls": total,
        "run_started_at": run_started.isoformat(),
        "sources_checked": len(config.news_sources),
        "feed_pages_requested": feed_pages or "adaptive",
        "coverage_counts": coverage_counts,
        "coverage_by_source": coverage_by_source,
        "failures_by_source": failures_by_source,
    }


if __name__ == "__main__":
    run_discovery()
