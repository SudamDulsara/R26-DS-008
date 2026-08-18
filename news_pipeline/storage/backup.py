from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inspect_backup(path: Path) -> dict:
    connection = sqlite3.connect(
        f"file:{path.as_posix()}?mode=ro",
        uri=True,
        timeout=30,
    )
    try:
        quick_check = str(
            connection.execute("PRAGMA quick_check").fetchone()[0]
        )
        foreign_key_violations = len(
            connection.execute("PRAGMA foreign_key_check").fetchall()
        )
        table_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                """
            ).fetchone()[0]
        )
        article_count = int(
            connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        )
        pipeline_run_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM pipeline_runs"
            ).fetchone()[0]
        )
    finally:
        connection.close()
    return {
        "quick_check": quick_check,
        "foreign_key_violations": foreign_key_violations,
        "table_count": table_count,
        "article_count": article_count,
        "pipeline_run_count": pipeline_run_count,
    }


def create_verified_backup(
    *,
    config: Optional[PipelineConfig] = None,
    output_path: Optional[Path] = None,
) -> dict:
    """Create an atomic SQLite online backup and prove it can be reopened."""
    selected_config = config or load_config()
    backup_dir = selected_config.data_dir / "backups"
    selected_path = output_path or (
        backup_dir
        / (
            "news_pipeline_"
            + datetime.now().astimezone().strftime("%Y-%m-%d_%H-%M-%S-%f")
            + ".db"
        )
    )
    selected_path = Path(selected_path).resolve()
    selected_path.parent.mkdir(parents=True, exist_ok=True)
    if selected_path.exists():
        raise FileExistsError(f"refusing to overwrite backup: {selected_path}")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{selected_path.name}-",
        suffix=".tmp",
        dir=selected_path.parent,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        source = get_connection(selected_config)
        destination = sqlite3.connect(temporary_path, timeout=30)
        try:
            source.backup(destination)
            destination.commit()
        finally:
            destination.close()
            source.close()
        inspection = _inspect_backup(temporary_path)
        if inspection["quick_check"] != "ok":
            raise RuntimeError(
                "backup failed SQLite quick_check: "
                f"{inspection['quick_check']}"
            )
        os.replace(temporary_path, selected_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    return {
        "backup_path": str(selected_path),
        "size_bytes": selected_path.stat().st_size,
        "sha256": _sha256(selected_path),
        "restore_read_check": "ok",
        **inspection,
    }
