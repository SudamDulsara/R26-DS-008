import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_NEWS_SOURCES = {
    "Mawbima": "https://www.mawbima.lk/feed",
    "Divaina": "https://www.divaina.lk/feed",
    "Silumina": "https://www.silumina.lk/feed",
    "BBC Sinhala": "https://www.bbc.com/sinhala/index.xml",
    "Ada Derana Sinhala": "https://sinhala.adaderana.lk/rsshotnews.php",
    "Anidda": "https://www.anidda.lk/feed",
    "NethnewsLk": "https://www.nethnews.lk/feed",
    "Navaliya": "https://www.navaliya.lk/feed",
    "Dinamina": "https://www.dinamina.lk/feed",
}


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path
    data_dir: Path
    logs_dir: Path
    snapshots_dir: Path
    db_path: Path
    max_retries: int
    retry_delay_seconds: int
    min_article_length: int
    purity_threshold: float
    news_sources: dict[str, str]


def _int_from_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


def _float_from_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


def load_config() -> PipelineConfig:
    project_root = Path(__file__).resolve().parent.parent
    data_dir = Path(os.getenv("NEWS_PIPELINE_DATA_DIR", project_root / "data"))
    logs_dir = Path(os.getenv("NEWS_PIPELINE_LOGS_DIR", project_root / "logs"))
    snapshots_dir = Path(
        os.getenv("NEWS_PIPELINE_SNAPSHOTS_DIR", data_dir / "snapshots")
    )
    db_path = Path(os.getenv("NEWS_PIPELINE_DB_PATH", data_dir / "news_pipeline.db"))

    return PipelineConfig(
        project_root=project_root,
        data_dir=data_dir,
        logs_dir=logs_dir,
        snapshots_dir=snapshots_dir,
        db_path=db_path,
        max_retries=_int_from_env("NEWS_PIPELINE_MAX_RETRIES", 3),
        retry_delay_seconds=_int_from_env("NEWS_PIPELINE_RETRY_DELAY", 3),
        min_article_length=_int_from_env("NEWS_PIPELINE_MIN_ARTICLE_LENGTH", 50),
        purity_threshold=_float_from_env("NEWS_PIPELINE_PURITY_THRESHOLD", 0.85),
        news_sources=DEFAULT_NEWS_SOURCES.copy(),
    )
