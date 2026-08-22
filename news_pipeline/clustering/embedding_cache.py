import json
import math
from datetime import datetime


def load_cached_vectors(
    cursor,
    *,
    input_fingerprints: list[str],
    model_name: str,
    model_revision: str,
) -> dict[str, list[float]]:
    """Load valid vectors for one exact model snapshot.

    Invalid or truncated rows are deliberately treated as misses so the
    clusterer can replace them from the pinned model instead of scoring with a
    partial vector.
    """
    unique_fingerprints = sorted(set(input_fingerprints))
    cached: dict[str, list[float]] = {}
    for start in range(0, len(unique_fingerprints), 500):
        batch = unique_fingerprints[start : start + 500]
        if not batch:
            continue
        placeholders = ",".join("?" for _ in batch)
        rows = cursor.execute(
            f"""
            SELECT input_fingerprint_sha256, dimensions, vector_json
            FROM clustering_embedding_cache
            WHERE model_name = ?
              AND model_revision = ?
              AND input_fingerprint_sha256 IN ({placeholders})
            """,
            (model_name, model_revision, *batch),
        ).fetchall()
        for row in rows:
            vector = _decode_vector(row["vector_json"], row["dimensions"])
            if vector is not None:
                cached[str(row["input_fingerprint_sha256"])] = vector
    return cached


def persist_cached_vectors(
    cursor,
    *,
    vectors_by_fingerprint: dict[str, list[float]],
    model_name: str,
    model_revision: str,
) -> None:
    if not vectors_by_fingerprint:
        return
    now = datetime.now().isoformat(timespec="seconds")
    cursor.executemany(
        """
        INSERT INTO clustering_embedding_cache (
            input_fingerprint_sha256,
            model_name,
            model_revision,
            dimensions,
            vector_json,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (
            input_fingerprint_sha256,
            model_name,
            model_revision
        ) DO UPDATE SET
            dimensions = excluded.dimensions,
            vector_json = excluded.vector_json,
            updated_at = excluded.updated_at
        """,
        [
            (
                fingerprint,
                model_name,
                model_revision,
                len(vector),
                json.dumps(vector, ensure_ascii=False, separators=(",", ":")),
                now,
                now,
            )
            for fingerprint, vector in vectors_by_fingerprint.items()
            if vector
        ],
    )


def _decode_vector(value, dimensions) -> list[float] | None:
    try:
        parsed = json.loads(str(value))
        expected_dimensions = int(dimensions)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, list) or len(parsed) != expected_dimensions:
        return None
    if not parsed:
        return None
    vector: list[float] = []
    for item in parsed:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            return None
        number = float(item)
        if not math.isfinite(number):
            return None
        vector.append(number)
    return vector
