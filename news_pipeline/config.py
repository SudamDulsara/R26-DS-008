import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from dotenv import dotenv_values


GPT_REASONING_EFFORTS = ("none", "low", "medium", "high", "xhigh", "max")
GPT_AUDIT_POLICY_MODES = ("all", "shadow", "risk_tiered")

SETTING_SPECS: dict[str, tuple[str, str]] = {
    "data_dir": ("NEWS_PIPELINE_DATA_DIR", "path"),
    "logs_dir": ("NEWS_PIPELINE_LOGS_DIR", "path"),
    "snapshots_dir": ("NEWS_PIPELINE_SNAPSHOTS_DIR", "path"),
    "reviews_dir": ("NEWS_PIPELINE_REVIEWS_DIR", "path"),
    "db_path": ("NEWS_PIPELINE_DB_PATH", "path"),
    "discovery_overlap_hours": (
        "NEWS_PIPELINE_DISCOVERY_OVERLAP_HOURS",
        "nonnegative_int",
    ),
    "discovery_max_pages": (
        "NEWS_PIPELINE_DISCOVERY_MAX_PAGES",
        "positive_int",
    ),
    "discovery_max_catchup_days": (
        "NEWS_PIPELINE_DISCOVERY_MAX_CATCHUP_DAYS",
        "positive_int",
    ),
    "max_retries": ("NEWS_PIPELINE_MAX_RETRIES", "nonnegative_int"),
    "retry_delay_seconds": (
        "NEWS_PIPELINE_RETRY_DELAY",
        "nonnegative_int",
    ),
    "extraction_recovery_passes": (
        "NEWS_PIPELINE_EXTRACTION_RECOVERY_PASSES",
        "positive_int",
    ),
    "extraction_workers": (
        "NEWS_PIPELINE_EXTRACTION_WORKERS",
        "positive_int",
    ),
    "min_article_length": (
        "NEWS_PIPELINE_MIN_ARTICLE_LENGTH",
        "positive_int",
    ),
    "purity_threshold": ("NEWS_PIPELINE_PURITY_THRESHOLD", "unit_float"),
    "cluster_model_name": ("NEWS_PIPELINE_CLUSTER_MODEL", "string"),
    "cluster_model_revision": (
        "NEWS_PIPELINE_CLUSTER_MODEL_REVISION",
        "optional_string",
    ),
    "cluster_similarity_threshold": (
        "NEWS_PIPELINE_CLUSTER_SIMILARITY_THRESHOLD",
        "similarity_float",
    ),
    "cluster_representative_threshold": (
        "NEWS_PIPELINE_CLUSTER_REPRESENTATIVE_THRESHOLD",
        "similarity_float",
    ),
    "cluster_cohesion_threshold": (
        "NEWS_PIPELINE_CLUSTER_COHESION_THRESHOLD",
        "similarity_float",
    ),
    "cluster_window_hours": (
        "NEWS_PIPELINE_CLUSTER_WINDOW_HOURS",
        "positive_int",
    ),
    "cluster_min_articles": (
        "NEWS_PIPELINE_CLUSTER_MIN_ARTICLES",
        "positive_int",
    ),
    "cluster_lead_char_limit": (
        "NEWS_PIPELINE_CLUSTER_LEAD_CHAR_LIMIT",
        "positive_int",
    ),
    "cluster_batch_size": (
        "NEWS_PIPELINE_CLUSTER_BATCH_SIZE",
        "positive_int",
    ),
    "cluster_min_lexical_overlap": (
        "NEWS_PIPELINE_CLUSTER_MIN_LEXICAL_OVERLAP",
        "unit_float",
    ),
    "cluster_allow_same_source_pairs": (
        "NEWS_PIPELINE_CLUSTER_ALLOW_SAME_SOURCE_PAIRS",
        "bool",
    ),
    "gpt_enabled": ("NEWS_PIPELINE_GPT_ENABLED", "bool"),
    "gpt_shadow_mode": ("NEWS_PIPELINE_GPT_SHADOW_MODE", "bool"),
    "gpt_only_publication_enabled": (
        "NEWS_PIPELINE_GPT_ONLY_PUBLICATION_ENABLED",
        "bool",
    ),
    "gpt_only_publication_prompt_version": (
        "NEWS_PIPELINE_GPT_ONLY_PUBLICATION_PROMPT_VERSION",
        "string",
    ),
    "gpt_model": ("NEWS_PIPELINE_GPT_MODEL", "string"),
    "gpt_prompt_version": (
        "NEWS_PIPELINE_GPT_PROMPT_VERSION",
        "string",
    ),
    "gpt_schema_version": (
        "NEWS_PIPELINE_GPT_SCHEMA_VERSION",
        "string",
    ),
    "gpt_reasoning_effort": (
        "NEWS_PIPELINE_GPT_REASONING_EFFORT",
        "reasoning_choice",
    ),
    "gpt_max_retries": (
        "NEWS_PIPELINE_GPT_MAX_RETRIES",
        "nonnegative_int",
    ),
    "gpt_max_output_tokens": (
        "NEWS_PIPELINE_GPT_MAX_OUTPUT_TOKENS",
        "positive_int",
    ),
    "gpt_timeout_seconds": (
        "NEWS_PIPELINE_GPT_TIMEOUT_SECONDS",
        "positive_float",
    ),
    "gpt_max_cost_per_story_usd": (
        "NEWS_PIPELINE_GPT_MAX_COST_PER_STORY_USD",
        "nonnegative_float",
    ),
    "gpt_max_cost_per_run_usd": (
        "NEWS_PIPELINE_GPT_MAX_COST_PER_RUN_USD",
        "nonnegative_float",
    ),
    "gpt_max_cost_per_day_usd": (
        "NEWS_PIPELINE_GPT_MAX_COST_PER_DAY_USD",
        "nonnegative_float",
    ),
    "gpt_max_cost_per_month_usd": (
        "NEWS_PIPELINE_GPT_MAX_COST_PER_MONTH_USD",
        "nonnegative_float",
    ),
    "gpt_max_clusters_per_run": (
        "NEWS_PIPELINE_GPT_MAX_CLUSTERS_PER_RUN",
        "positive_int",
    ),
    "gpt_autonomous_audit_enabled": (
        "NEWS_PIPELINE_GPT_AUTONOMOUS_AUDIT_ENABLED",
        "bool",
    ),
    "gpt_audit_policy_mode": (
        "NEWS_PIPELINE_GPT_AUDIT_POLICY_MODE",
        "audit_policy_choice",
    ),
    "gpt_low_risk_audit_sample_rate": (
        "NEWS_PIPELINE_GPT_LOW_RISK_AUDIT_SAMPLE_RATE",
        "unit_float",
    ),
    "gpt_audit_circuit_min_evaluated": (
        "NEWS_PIPELINE_GPT_AUDIT_CIRCUIT_MIN_EVALUATED",
        "positive_int",
    ),
    "gpt_audit_circuit_max_material_rate": (
        "NEWS_PIPELINE_GPT_AUDIT_CIRCUIT_MAX_MATERIAL_RATE",
        "unit_float",
    ),
    "gpt_audit_high_risk_article_count": (
        "NEWS_PIPELINE_GPT_AUDIT_HIGH_RISK_ARTICLE_COUNT",
        "positive_int",
    ),
    "gpt_audit_high_risk_source_count": (
        "NEWS_PIPELINE_GPT_AUDIT_HIGH_RISK_SOURCE_COUNT",
        "positive_int",
    ),
    "gpt_audit_high_risk_evidence_chars": (
        "NEWS_PIPELINE_GPT_AUDIT_HIGH_RISK_EVIDENCE_CHARS",
        "positive_int",
    ),
    "gpt_audit_medium_risk_evidence_chars": (
        "NEWS_PIPELINE_GPT_AUDIT_MEDIUM_RISK_EVIDENCE_CHARS",
        "positive_int",
    ),
    "gpt_audit_model": ("NEWS_PIPELINE_GPT_AUDIT_MODEL", "string"),
    "gpt_audit_complex_model": (
        "NEWS_PIPELINE_GPT_AUDIT_COMPLEX_MODEL",
        "string",
    ),
    "gpt_audit_reasoning_effort": (
        "NEWS_PIPELINE_GPT_AUDIT_REASONING_EFFORT",
        "reasoning_choice",
    ),
    "gpt_audit_complex_reasoning_effort": (
        "NEWS_PIPELINE_GPT_AUDIT_COMPLEX_REASONING_EFFORT",
        "reasoning_choice",
    ),
    "gpt_audit_max_output_tokens": (
        "NEWS_PIPELINE_GPT_AUDIT_MAX_OUTPUT_TOKENS",
        "positive_int",
    ),
}


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path
    data_dir: Path
    logs_dir: Path
    snapshots_dir: Path
    reviews_dir: Path
    db_path: Path
    discovery_overlap_hours: int
    discovery_max_pages: int
    discovery_max_catchup_days: int
    max_retries: int
    retry_delay_seconds: int
    extraction_recovery_passes: int
    extraction_workers: int
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
    gpt_max_cost_per_day_usd: float
    gpt_max_cost_per_month_usd: float
    gpt_max_clusters_per_run: int
    gpt_autonomous_audit_enabled: bool
    gpt_audit_policy_mode: str
    gpt_low_risk_audit_sample_rate: float
    gpt_audit_circuit_min_evaluated: int
    gpt_audit_circuit_max_material_rate: float
    gpt_audit_high_risk_article_count: int
    gpt_audit_high_risk_source_count: int
    gpt_audit_high_risk_evidence_chars: int
    gpt_audit_medium_risk_evidence_chars: int
    gpt_audit_model: str
    gpt_audit_complex_model: str
    gpt_audit_reasoning_effort: str
    gpt_audit_complex_reasoning_effort: str
    gpt_audit_max_output_tokens: int
    openai_api_key: Optional[str] = field(repr=False)
    news_sources: dict[str, str]


def _stringify_settings(settings: Mapping[str, Any]) -> dict[str, str]:
    environment: dict[str, str] = {}
    for key, value in settings.items():
        if isinstance(value, bool):
            environment[str(key)] = str(value).lower()
        elif isinstance(value, (str, int, float)):
            environment[str(key)] = str(value)
        else:
            raise ValueError(f"pipeline setting {key} must be scalar")
    return environment


def _load_settings(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(f"pipeline configuration file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("pipeline configuration must be a JSON object")
    settings = payload.get("settings")
    if not isinstance(settings, Mapping):
        raise ValueError("pipeline settings must be a JSON object")

    expected = {spec[0] for spec in SETTING_SPECS.values()}
    actual = {str(key) for key in settings}
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing:
        raise ValueError(f"pipeline configuration is missing settings: {missing}")
    if unknown:
        raise ValueError(f"pipeline configuration has unknown settings: {unknown}")

    source_payload = payload.get("news_sources")
    if not isinstance(source_payload, Mapping):
        raise ValueError("news_sources must be a JSON object")
    sources = {
        str(name).strip(): str(url).strip()
        for name, url in source_payload.items()
        if str(name).strip() and str(url).strip()
    }
    if not sources:
        raise ValueError("news_sources must not be empty")
    return _stringify_settings(settings), sources


def _configuration_environment(
    settings: Mapping[str, str],
    *,
    project_root: Path,
    load_env_file: bool,
    env_file: Optional[Path],
) -> dict[str, str]:
    environment = dict(settings)
    if load_env_file:
        selected_env_file = env_file or project_root / ".env"
        file_values: Mapping[str, Optional[str]] = dotenv_values(selected_env_file)
        api_key = file_values.get("OPENAI_API_KEY")
        if api_key is not None:
            environment["OPENAI_API_KEY"] = api_key

    supported = {spec[0] for spec in SETTING_SPECS.values()}
    for name in supported | {"OPENAI_API_KEY"}:
        if name in os.environ:
            environment[name] = os.environ[name]
    return environment


def _parse_bool(name: str, value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _parse_number(name: str, value: str, kind: str) -> int | float:
    try:
        parsed: int | float
        parsed = int(value) if kind.endswith("int") else float(value)
    except ValueError as exc:
        raise ValueError(f"{name} has an invalid numeric value") from exc
    if kind.startswith("positive") and parsed <= 0:
        raise ValueError(f"{name} must be positive")
    if kind.startswith("nonnegative") and parsed < 0:
        raise ValueError(f"{name} must be nonnegative")
    if kind == "unit_float" and not 0.0 <= parsed <= 1.0:
        raise ValueError(f"{name} must be between 0 and 1")
    if kind == "similarity_float" and not -1.0 <= parsed <= 1.0:
        raise ValueError(f"{name} must be between -1 and 1")
    return parsed


def _parse_setting(
    *,
    project_root: Path,
    environment: Mapping[str, str],
    name: str,
    kind: str,
) -> Any:
    value = environment[name]
    if kind == "path":
        path = Path(value)
        return path if path.is_absolute() else project_root / path
    if kind == "bool":
        return _parse_bool(name, value)
    if kind in {
        "positive_int",
        "nonnegative_int",
        "positive_float",
        "nonnegative_float",
        "unit_float",
        "similarity_float",
    }:
        return _parse_number(name, value, kind)
    if kind == "optional_string":
        return value.strip() or None
    if kind == "reasoning_choice":
        normalized = value.strip().lower()
        if normalized not in GPT_REASONING_EFFORTS:
            raise ValueError(f"{name} must be one of {GPT_REASONING_EFFORTS}")
        return normalized
    if kind == "audit_policy_choice":
        normalized = value.strip().lower()
        if normalized not in GPT_AUDIT_POLICY_MODES:
            raise ValueError(f"{name} must be one of {GPT_AUDIT_POLICY_MODES}")
        return normalized
    if kind == "string":
        parsed = value.strip()
        if not parsed:
            raise ValueError(f"{name} must not be empty")
        return parsed
    raise ValueError(f"unsupported configuration parser: {kind}")


def load_config(
    *,
    load_env_file: bool = True,
    env_file: Optional[Path] = None,
    settings_file: Optional[Path] = None,
) -> PipelineConfig:
    project_root = Path(__file__).resolve().parent.parent
    selected_settings_file = settings_file or (
        Path(__file__).resolve().parent / "pipeline_config.json"
    )
    settings, news_sources = _load_settings(selected_settings_file)
    environment = _configuration_environment(
        settings,
        project_root=project_root,
        load_env_file=load_env_file,
        env_file=env_file,
    )
    operational_values = {
        field_name: _parse_setting(
            project_root=project_root,
            environment=environment,
            name=setting_name,
            kind=kind,
        )
        for field_name, (setting_name, kind) in SETTING_SPECS.items()
    }
    configured_model_name = settings["NEWS_PIPELINE_CLUSTER_MODEL"].strip()
    selected_model_name = str(operational_values["cluster_model_name"])
    if (
        selected_model_name != configured_model_name
        and "NEWS_PIPELINE_CLUSTER_MODEL" in os.environ
        and "NEWS_PIPELINE_CLUSTER_MODEL_REVISION" not in os.environ
    ):
        # A revision is model-specific. An explicit environment override of the
        # model must not silently inherit the committed default model's pin.
        operational_values["cluster_model_revision"] = None
    api_key = environment.get("OPENAI_API_KEY")
    return PipelineConfig(
        project_root=project_root,
        **operational_values,
        openai_api_key=api_key.strip() if api_key and api_key.strip() else None,
        news_sources=news_sources,
    )
