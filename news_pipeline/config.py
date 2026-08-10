import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from dotenv import dotenv_values


DEFAULT_CLUSTER_MODEL = "intfloat/multilingual-e5-base"
DEFAULT_CLUSTER_MODEL_REVISION = (
    "d128750597153bb5987e10b1c3493a34e5a4502a"
)
DEFAULT_GPT_MODEL = "gpt-5.6-luna"
DEFAULT_GPT_PROMPT_VERSION = "unified_story_prompt_v2_1"
DEFAULT_GPT_SCHEMA_VERSION = "unified_story_schema_v2"
GPT_REASONING_EFFORTS = ("none", "low", "medium", "high", "xhigh", "max")
DEFAULT_GPT_REASONING_EFFORT = "low"
DEFAULT_GPT_MAX_RETRIES = 2
DEFAULT_GPT_MAX_OUTPUT_TOKENS = 4096
DEFAULT_GPT_TIMEOUT_SECONDS = 120.0
DEFAULT_GPT_MAX_COST_PER_STORY_USD = 1.0
DEFAULT_GPT_MAX_COST_PER_RUN_USD = 10.0


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
    reviews_dir: Path
    db_path: Path
    max_retries: int
    retry_delay_seconds: int
    min_article_length: int
    purity_threshold: float
    cluster_model_name: str
    cluster_model_revision: Optional[str]
    cluster_similarity_threshold: float
    cluster_representative_threshold: float
    cluster_cohesion_threshold: float
    cluster_window_hours: int
    cluster_min_articles: int
    cluster_lead_char_limit: int
    cluster_batch_size: int
    cluster_min_lexical_overlap: float
    cluster_allow_same_source_pairs: bool
    gpt_enabled: bool
    gpt_shadow_mode: bool
    gpt_only_publication_enabled: bool
    gpt_only_publication_prompt_version: str
    gpt_model: str
    gpt_prompt_version: str
    gpt_schema_version: str
    gpt_reasoning_effort: str
    gpt_max_retries: int
    gpt_max_output_tokens: int
    gpt_timeout_seconds: float
    gpt_max_cost_per_story_usd: float
    gpt_max_cost_per_run_usd: float
    openai_api_key: Optional[str] = field(repr=False)
    news_sources: dict[str, str]


def _int_from_env(
    environment: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    try:
        return int(environment.get(name, default))
    except ValueError:
        return default


def _float_from_env(
    environment: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    try:
        return float(environment.get(name, default))
    except ValueError:
        return default


def _bool_from_env(
    environment: Mapping[str, str],
    name: str,
    default: bool,
) -> bool:
    value = environment.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _optional_string_from_env(
    environment: Mapping[str, str],
    name: str,
    default: Optional[str],
) -> Optional[str]:
    value = environment.get(name)
    if value is None:
        return default
    return value.strip() or None


def _choice_from_env(
    environment: Mapping[str, str],
    name: str,
    default: str,
    choices: tuple[str, ...],
) -> str:
    value = str(environment.get(name, default)).strip().lower()
    return value if value in choices else default


def _nonnegative_int_from_env(
    environment: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    value = _int_from_env(environment, name, default)
    return value if value >= 0 else default


def _positive_int_from_env(
    environment: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    value = _int_from_env(environment, name, default)
    return value if value > 0 else default


def _nonnegative_float_from_env(
    environment: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    value = _float_from_env(environment, name, default)
    return value if value >= 0.0 else default


def _positive_float_from_env(
    environment: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    value = _float_from_env(environment, name, default)
    return value if value > 0.0 else default


def _configuration_environment(
    project_root: Path,
    *,
    load_env_file: bool,
    env_file: Optional[Path],
) -> dict[str, str]:
    environment = {}
    if load_env_file:
        selected_env_file = env_file or project_root / ".env"
        file_values: Mapping[str, Optional[str]] = dotenv_values(
            selected_env_file
        )
        environment.update(
            {
                key: value
                for key, value in file_values.items()
                if value is not None
            }
        )
    # A real process environment always takes precedence over local .env.
    environment.update(os.environ)
    return environment


def _environment_value(
    environment: Mapping[str, str],
    name: str,
    default: Any,
) -> Any:
    return environment.get(name, default)


def load_config(
    *,
    load_env_file: bool = True,
    env_file: Optional[Path] = None,
) -> PipelineConfig:
    project_root = Path(__file__).resolve().parent.parent
    environment = _configuration_environment(
        project_root,
        load_env_file=load_env_file,
        env_file=env_file,
    )
    data_dir = Path(
        _environment_value(
            environment,
            "NEWS_PIPELINE_DATA_DIR",
            project_root / "data",
        )
    )
    logs_dir = Path(
        _environment_value(
            environment,
            "NEWS_PIPELINE_LOGS_DIR",
            project_root / "logs",
        )
    )
    snapshots_dir = Path(
        _environment_value(
            environment,
            "NEWS_PIPELINE_SNAPSHOTS_DIR",
            data_dir / "snapshots",
        )
    )
    reviews_dir = Path(
        _environment_value(
            environment,
            "NEWS_PIPELINE_REVIEWS_DIR",
            data_dir / "reviews",
        )
    )
    db_path = Path(
        _environment_value(
            environment,
            "NEWS_PIPELINE_DB_PATH",
            data_dir / "news_pipeline.db",
        )
    )
    cluster_similarity_threshold = _float_from_env(
        environment,
        "NEWS_PIPELINE_CLUSTER_SIMILARITY_THRESHOLD",
        0.92,
    )
    cluster_representative_threshold = _float_from_env(
        environment,
        "NEWS_PIPELINE_CLUSTER_REPRESENTATIVE_THRESHOLD",
        cluster_similarity_threshold,
    )
    default_cohesion_threshold = max(
        -1.0,
        round(cluster_representative_threshold - 0.02, 6),
    )
    cluster_model_name = _environment_value(
        environment,
        "NEWS_PIPELINE_CLUSTER_MODEL",
        DEFAULT_CLUSTER_MODEL,
    )
    default_model_revision = (
        DEFAULT_CLUSTER_MODEL_REVISION
        if cluster_model_name == DEFAULT_CLUSTER_MODEL
        else None
    )

    return PipelineConfig(
        project_root=project_root,
        data_dir=data_dir,
        logs_dir=logs_dir,
        snapshots_dir=snapshots_dir,
        reviews_dir=reviews_dir,
        db_path=db_path,
        max_retries=_int_from_env(
            environment,
            "NEWS_PIPELINE_MAX_RETRIES",
            3,
        ),
        retry_delay_seconds=_int_from_env(
            environment,
            "NEWS_PIPELINE_RETRY_DELAY",
            3,
        ),
        min_article_length=_int_from_env(
            environment,
            "NEWS_PIPELINE_MIN_ARTICLE_LENGTH",
            50,
        ),
        purity_threshold=_float_from_env(
            environment,
            "NEWS_PIPELINE_PURITY_THRESHOLD",
            0.85,
        ),
        cluster_model_name=cluster_model_name,
        cluster_model_revision=_optional_string_from_env(
            environment,
            "NEWS_PIPELINE_CLUSTER_MODEL_REVISION",
            default_model_revision,
        ),
        cluster_similarity_threshold=cluster_similarity_threshold,
        cluster_representative_threshold=cluster_representative_threshold,
        cluster_cohesion_threshold=_float_from_env(
            environment,
            "NEWS_PIPELINE_CLUSTER_COHESION_THRESHOLD",
            default_cohesion_threshold,
        ),
        cluster_window_hours=_int_from_env(
            environment,
            "NEWS_PIPELINE_CLUSTER_WINDOW_HOURS",
            72,
        ),
        cluster_min_articles=_int_from_env(
            environment,
            "NEWS_PIPELINE_CLUSTER_MIN_ARTICLES",
            2,
        ),
        cluster_lead_char_limit=_int_from_env(
            environment,
            "NEWS_PIPELINE_CLUSTER_LEAD_CHAR_LIMIT",
            900,
        ),
        cluster_batch_size=_int_from_env(
            environment,
            "NEWS_PIPELINE_CLUSTER_BATCH_SIZE",
            16,
        ),
        cluster_min_lexical_overlap=_float_from_env(
            environment,
            "NEWS_PIPELINE_CLUSTER_MIN_LEXICAL_OVERLAP",
            0.0,
        ),
        cluster_allow_same_source_pairs=_bool_from_env(
            environment,
            "NEWS_PIPELINE_CLUSTER_ALLOW_SAME_SOURCE_PAIRS",
            False,
        ),
        gpt_enabled=_bool_from_env(
            environment,
            "NEWS_PIPELINE_GPT_ENABLED",
            False,
        ),
        gpt_shadow_mode=_bool_from_env(
            environment,
            "NEWS_PIPELINE_GPT_SHADOW_MODE",
            False,
        ),
        gpt_only_publication_enabled=_bool_from_env(
            environment,
            "NEWS_PIPELINE_GPT_ONLY_PUBLICATION_ENABLED",
            False,
        ),
        gpt_only_publication_prompt_version=(
            _optional_string_from_env(
                environment,
                "NEWS_PIPELINE_GPT_ONLY_PUBLICATION_PROMPT_VERSION",
                "unified_story_prompt_v2_8",
            )
            or "unified_story_prompt_v2_8"
        ),
        gpt_model=(
            _optional_string_from_env(
                environment,
                "NEWS_PIPELINE_GPT_MODEL",
                DEFAULT_GPT_MODEL,
            )
            or DEFAULT_GPT_MODEL
        ),
        gpt_prompt_version=(
            _optional_string_from_env(
                environment,
                "NEWS_PIPELINE_GPT_PROMPT_VERSION",
                DEFAULT_GPT_PROMPT_VERSION,
            )
            or DEFAULT_GPT_PROMPT_VERSION
        ),
        gpt_schema_version=(
            _optional_string_from_env(
                environment,
                "NEWS_PIPELINE_GPT_SCHEMA_VERSION",
                DEFAULT_GPT_SCHEMA_VERSION,
            )
            or DEFAULT_GPT_SCHEMA_VERSION
        ),
        gpt_reasoning_effort=_choice_from_env(
            environment,
            "NEWS_PIPELINE_GPT_REASONING_EFFORT",
            DEFAULT_GPT_REASONING_EFFORT,
            GPT_REASONING_EFFORTS,
        ),
        gpt_max_retries=_nonnegative_int_from_env(
            environment,
            "NEWS_PIPELINE_GPT_MAX_RETRIES",
            DEFAULT_GPT_MAX_RETRIES,
        ),
        gpt_max_output_tokens=_positive_int_from_env(
            environment,
            "NEWS_PIPELINE_GPT_MAX_OUTPUT_TOKENS",
            DEFAULT_GPT_MAX_OUTPUT_TOKENS,
        ),
        gpt_timeout_seconds=_positive_float_from_env(
            environment,
            "NEWS_PIPELINE_GPT_TIMEOUT_SECONDS",
            DEFAULT_GPT_TIMEOUT_SECONDS,
        ),
        gpt_max_cost_per_story_usd=_nonnegative_float_from_env(
            environment,
            "NEWS_PIPELINE_GPT_MAX_COST_PER_STORY_USD",
            DEFAULT_GPT_MAX_COST_PER_STORY_USD,
        ),
        gpt_max_cost_per_run_usd=_nonnegative_float_from_env(
            environment,
            "NEWS_PIPELINE_GPT_MAX_COST_PER_RUN_USD",
            DEFAULT_GPT_MAX_COST_PER_RUN_USD,
        ),
        openai_api_key=_optional_string_from_env(
            environment,
            "OPENAI_API_KEY",
            None,
        ),
        news_sources=DEFAULT_NEWS_SOURCES.copy(),
    )
