from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger
from news_pipeline.unification.final_publication import (
    materialize_gpt_only_publication,
)


CURRENT_EXPORT_VERSION = "gpt_only_current_publication_v1"
PERMISSION_RETRY_DELAYS_SECONDS = (0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0)
T = TypeVar("T")


def _retry_permission_error(action: Callable[[], T]) -> T:
    for delay in (*PERMISSION_RETRY_DELAYS_SECONDS, None):
        try:
            return action()
        except PermissionError:
            if delay is None:
                raise
            time.sleep(delay)
    raise AssertionError("permission retry loop did not return or raise")


def _read_json_object(path: Path) -> Optional[dict[str, Any]]:
    try:
        raw_value = _retry_permission_error(
            lambda: path.read_text(encoding="utf-8")
        )
        value = json.loads(raw_value)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _write_publication_state(
    path: Path,
    manifest: dict[str, Any],
) -> None:
    state = dict(manifest)
    state.pop("manifest_sha256", None)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}-",
        suffix=".tmp",
        dir=path.parent,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        temporary_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        _retry_permission_error(lambda: os.replace(temporary_path, path))
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _remove_controlled_directory(path: Path, data_dir: Path) -> None:
    if path.parent.resolve() != data_dir.resolve():
        raise RuntimeError(f"refusing to remove uncontrolled path: {path}")
    if path.exists():
        if not path.is_dir():
            raise RuntimeError(f"expected a directory: {path}")
        shutil.rmtree(path)


def _publication_files(path: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    children = _retry_permission_error(lambda: list(path.iterdir()))
    for child in children:
        if not child.is_file():
            raise RuntimeError(
                f"publication bundles must contain only files: {child}"
            )
        files[child.name] = child
    return files


def _publish_files_in_place(
    *,
    staging_dir: Path,
    current_dir: Path,
    previous_dir: Path,
) -> None:
    """Publish when Windows has an open handle on ``current_dir``.

    Windows can reject renaming a directory that is being watched or served,
    even when none of its files are open. Keep the directory in place in that
    case and atomically replace each file, with the manifest replaced last so
    readers can use it as the bundle's commit marker.
    """
    current_files = _publication_files(current_dir)
    staging_files = _publication_files(staging_dir)
    previous_dir.mkdir()
    for name, path in current_files.items():
        _retry_permission_error(
            lambda path=path, name=name: shutil.copy2(
                path, previous_dir / name
            )
        )

    original_names = set(current_files)
    published_names: set[str] = set()
    manifest_name = "final_publication_manifest.json"
    publish_order = sorted(
        staging_files,
        key=lambda name: (name == manifest_name, name),
    )
    try:
        for name in publish_order:
            _retry_permission_error(
                lambda name=name: os.replace(
                    staging_files[name], current_dir / name
                )
            )
            published_names.add(name)
        for name in original_names - set(staging_files):
            (current_dir / name).unlink()
    except Exception:
        for name in published_names - original_names:
            published_path = current_dir / name
            if published_path.exists():
                published_path.unlink()
        for name in original_names:
            shutil.copy2(previous_dir / name, current_dir / name)
        raise

    shutil.rmtree(staging_dir)
    shutil.rmtree(previous_dir)


def _atomic_publish(
    *,
    staging_dir: Path,
    current_dir: Path,
    data_dir: Path,
) -> None:
    previous_dir = data_dir / ".current.previous"
    if current_dir.parent.resolve() != data_dir.resolve():
        raise RuntimeError("current publication must be inside data_dir")
    if staging_dir.parent.resolve() != data_dir.resolve():
        raise RuntimeError("staging publication must be inside data_dir")

    if previous_dir.exists():
        if current_dir.exists():
            _remove_controlled_directory(previous_dir, data_dir)
        else:
            os.replace(previous_dir, current_dir)

    moved_current = False
    try:
        if current_dir.exists():
            if not current_dir.is_dir():
                raise RuntimeError(f"expected a directory: {current_dir}")
            try:
                _retry_permission_error(
                    lambda: os.replace(current_dir, previous_dir)
                )
            except PermissionError:
                get_logger().warning(
                    "Could not rename live publication directory; "
                    "using file-level atomic replacement"
                )
                _publish_files_in_place(
                    staging_dir=staging_dir,
                    current_dir=current_dir,
                    previous_dir=previous_dir,
                )
                return
            moved_current = True
        os.replace(staging_dir, current_dir)
    except Exception:
        if moved_current and previous_dir.exists() and not current_dir.exists():
            os.replace(previous_dir, current_dir)
        raise
    else:
        _remove_controlled_directory(previous_dir, data_dir)


def _exported_unique_article_count(config: PipelineConfig) -> int:
    connection = get_connection(config)
    try:
        return int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM articles
                WHERE clean_status = 'cleaned'
                  AND dedupe_status = 'unique'
                """
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _result(
    *,
    current_dir: Path,
    manifest: dict[str, Any],
    exported_unique_articles: int,
    reused: bool,
) -> dict[str, Any]:
    counts = manifest["counts"]
    return {
        "export_mode": "current_gpt_only_publication",
        "full_snapshot_created": False,
        "reused_current_bundle": reused,
        "snapshot_dir": str(current_dir),
        "report_path": str(current_dir / "report.json"),
        "final_gpt_only_publication": manifest,
        "primary_unification_contract": "final_gpt_only_publication",
        "primary_unified_stories_path": manifest["paths"][
            "final_unified_stories"
        ],
        "exported_unique_articles": exported_unique_articles,
        "story_clusters": counts["eligible_clusters"],
        "unified_stories": counts["final_unified_stories"],
    }


def export_current_publication(
    *,
    config: Optional[PipelineConfig] = None,
) -> dict[str, Any]:
    """Atomically publish the routine GPT-only consumer bundle.

    Full dataset and rollback exports remain available through the explicit
    ``news_pipeline export`` command.
    """
    selected_config = config or load_config()
    if not selected_config.gpt_only_publication_enabled:
        raise RuntimeError(
            "current publication requires GPT-only publication to be enabled"
        )

    data_dir = selected_config.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    current_dir = data_dir / "current"
    publication_state_path = data_dir / ".current-publication-state.json"
    # ``tempfile.mkdtemp`` creates a private directory on Windows. After an
    # atomic directory swap that restrictive ACL becomes the ACL of
    # ``data/current``, preventing workspace readers from opening the
    # publication. A regular child directory inherits ``data_dir``'s ACL.
    staging_dir = data_dir / f".current-staging-{uuid.uuid4().hex}"
    staging_dir.mkdir()
    try:
        manifest = materialize_gpt_only_publication(
            output_dir=staging_dir,
            published_output_dir=current_dir,
            config=selected_config,
        )
        report = {
            "export_version": CURRENT_EXPORT_VERSION,
            "generated_at": datetime.now().astimezone().isoformat(
                timespec="seconds"
            ),
            "export_mode": "current_gpt_only_publication",
            "full_snapshot_created": False,
            "primary_unification_contract": "final_gpt_only_publication",
            "publication_fingerprint_sha256": manifest[
                "publication_fingerprint_sha256"
            ],
            "paths": {
                "primary_unified_stories": manifest["paths"][
                    "final_unified_stories"
                ],
                "final_gpt_only_publication": manifest["paths"],
            },
            "counts": manifest["counts"],
        }
        (staging_dir / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        exported_unique_articles = _exported_unique_article_count(
            selected_config
        )

        existing_manifest = _read_json_object(publication_state_path)
        if existing_manifest is None:
            existing_manifest = _read_json_object(
                current_dir / "final_publication_manifest.json"
            )
        if (
            existing_manifest is not None
            and current_dir.is_dir()
            and existing_manifest.get("publication_fingerprint_sha256")
            == manifest["publication_fingerprint_sha256"]
        ):
            _remove_controlled_directory(staging_dir, data_dir)
            _write_publication_state(
                publication_state_path,
                existing_manifest,
            )
            get_logger().info(
                "Current GPT-only publication is unchanged; reused %s",
                current_dir,
            )
            return _result(
                current_dir=current_dir,
                manifest=existing_manifest,
                exported_unique_articles=exported_unique_articles,
                reused=True,
            )

        _atomic_publish(
            staging_dir=staging_dir,
            current_dir=current_dir,
            data_dir=data_dir,
        )
        _write_publication_state(publication_state_path, manifest)
        get_logger().info(
            "Published current GPT-only bundle to %s", current_dir
        )
        return _result(
            current_dir=current_dir,
            manifest=manifest,
            exported_unique_articles=exported_unique_articles,
            reused=False,
        )
    except Exception:
        if staging_dir.exists():
            _remove_controlled_directory(staging_dir, data_dir)
        raise
