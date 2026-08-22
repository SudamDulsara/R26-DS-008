UNIFIED_STORY_CONTRACT_VERSION = "unified_story_v2_reviewed_2026-07-23"
UNIFIED_STORY_SCHEMA_VERSION = "extractive_v2"
EXTRACTIVE_SELECTION_METHOD = "extractive_lead_mmr_v10"
REVIEWED_BASELINE_SNAPSHOT = "2026-07-23_02-41-21"
REVIEWED_BASELINE_STORY_COUNT = 47
REVIEWED_BASELINE_STRICT_ACCEPTANCE = 1.0


STORY_REQUIRED_FIELDS = {
    "story_id",
    "cluster_id",
    "title",
    "display_title",
    "display_title_provenance",
    "clean_text",
    "unified_text",
    "unified_sentences",
    "has_conflict_flags",
    "conflict_flag_count",
    "conflict_detection_scope",
    "conflict_flags",
    "unification",
    "representative_article",
    "event_date_start",
    "event_date_end",
    "article_count",
    "source_count",
    "confidence",
    "clustering",
    "source_articles",
}

UNIFICATION_REQUIRED_FIELDS = {
    "version",
    "status",
    "selection_method",
    "selection_config",
    "candidate_group_count",
    "eligible_group_count",
    "selected_sentence_count",
    "character_count",
    "lead_anchor_used",
    "lead_anchor_sentence_id",
    "suppressed_orphan_speaker_introduction_count",
    "suppressed_orphan_speaker_introductions",
    "suppressed_orphan_context_sentence_count",
    "suppressed_orphan_context_sentences",
    "suppressed_heading_fragment_count",
    "suppressed_heading_fragments",
    "suppressed_relevance_sentence_count",
    "suppressed_relevance_sentences",
    "suppressed_residual_repetition_count",
    "suppressed_residual_repetitions",
    "fallback",
}

UNIFIED_SENTENCE_REQUIRED_FIELDS = {
    "selection_rank",
    "group_id",
    "sentence_id",
    "text",
    "selection_reason",
    "selection_score",
    "base_score",
    "redundancy_to_selected",
    "exact_support_count",
    "group_support_count",
    "group_variant_count",
    "supporting_articles",
    "near_duplicate_variants",
    "output_position",
    "conflict_flag_ids",
}

SUPPRESSION_COLLECTIONS = (
    (
        "suppressed_orphan_speaker_introduction_count",
        "suppressed_orphan_speaker_introductions",
    ),
    (
        "suppressed_orphan_context_sentence_count",
        "suppressed_orphan_context_sentences",
    ),
    (
        "suppressed_heading_fragment_count",
        "suppressed_heading_fragments",
    ),
    (
        "suppressed_relevance_sentence_count",
        "suppressed_relevance_sentences",
    ),
    (
        "suppressed_residual_repetition_count",
        "suppressed_residual_repetitions",
    ),
)


class UnifiedStoryContractError(ValueError):
    pass


def unified_story_contract_metadata() -> dict:
    """Return snapshot-level metadata for the reviewed output behavior.

    The review baseline certifies the algorithm and schema contract, not every
    future story produced from new input data.
    """
    return {
        "contract_version": UNIFIED_STORY_CONTRACT_VERSION,
        "schema_version": UNIFIED_STORY_SCHEMA_VERSION,
        "selection_method": EXTRACTIVE_SELECTION_METHOD,
        "review_baseline": {
            "snapshot_name": REVIEWED_BASELINE_SNAPSHOT,
            "story_count": REVIEWED_BASELINE_STORY_COUNT,
            "strict_acceptance_rate": REVIEWED_BASELINE_STRICT_ACCEPTANCE,
        },
    }


def validate_unified_story_record(story: dict) -> None:
    """Fail fast when an exported unified story breaks the reviewed contract."""
    _require_fields(story, STORY_REQUIRED_FIELDS, "unified story")
    unification = story["unification"]
    _require_fields(unification, UNIFICATION_REQUIRED_FIELDS, "unification")

    if unification["version"] != UNIFIED_STORY_SCHEMA_VERSION:
        raise UnifiedStoryContractError(
            "unexpected unified-story schema version: "
            f"{unification['version']!r}"
        )
    if unification["selection_method"] != EXTRACTIVE_SELECTION_METHOD:
        raise UnifiedStoryContractError(
            "unexpected extractive selection method: "
            f"{unification['selection_method']!r}"
        )
    if unification["status"] not in {
        "generated",
        "fallback_only",
        "unavailable",
    }:
        raise UnifiedStoryContractError(
            f"unexpected unification status: {unification['status']!r}"
        )

    sentences = story["unified_sentences"]
    for sentence in sentences:
        _require_fields(
            sentence,
            UNIFIED_SENTENCE_REQUIRED_FIELDS,
            "unified sentence",
        )

    expected_positions = list(range(1, len(sentences) + 1))
    actual_positions = [item["output_position"] for item in sentences]
    if actual_positions != expected_positions:
        raise UnifiedStoryContractError(
            "unified sentence output positions must be consecutive"
        )

    expected_text = "\n".join(item["text"] for item in sentences)
    if story["unified_text"] != expected_text:
        raise UnifiedStoryContractError(
            "unified_text must equal the ordered selected sentence texts"
        )
    if unification["selected_sentence_count"] != len(sentences):
        raise UnifiedStoryContractError(
            "selected_sentence_count does not match unified_sentences"
        )
    if unification["character_count"] != len(story["unified_text"]):
        raise UnifiedStoryContractError(
            "character_count does not match unified_text"
        )

    status = unification["status"]
    if status == "generated" and not story["unified_text"]:
        raise UnifiedStoryContractError(
            "generated unified stories must contain extractive text"
        )
    if status != "generated" and story["unified_text"]:
        raise UnifiedStoryContractError(
            f"{status} unified stories cannot contain extractive text"
        )

    if story["conflict_flag_count"] != len(story["conflict_flags"]):
        raise UnifiedStoryContractError(
            "conflict_flag_count does not match conflict_flags"
        )
    if story["has_conflict_flags"] != bool(story["conflict_flags"]):
        raise UnifiedStoryContractError(
            "has_conflict_flags does not match conflict_flags"
        )

    for count_field, collection_field in SUPPRESSION_COLLECTIONS:
        if unification[count_field] != len(unification[collection_field]):
            raise UnifiedStoryContractError(
                f"{count_field} does not match {collection_field}"
            )

    fallback = unification["fallback"]
    _require_fields(
        fallback,
        {"method", "content_field", "available"},
        "unification fallback",
    )
    if fallback["method"] != "representative_article_v1":
        raise UnifiedStoryContractError("unexpected fallback method")
    if fallback["content_field"] != "clean_text":
        raise UnifiedStoryContractError("unexpected fallback content field")
    if fallback["available"] != bool(story["clean_text"]):
        raise UnifiedStoryContractError(
            "fallback availability does not match clean_text"
        )

    source_article_ids = {
        item.get("article_id") for item in story["source_articles"]
    }
    selected_support_ids = {
        support.get("article_id")
        for sentence in sentences
        for support in sentence["supporting_articles"]
    }
    unknown_support_ids = selected_support_ids - source_article_ids
    if unknown_support_ids:
        raise UnifiedStoryContractError(
            "selected sentence support is outside the parent cluster: "
            f"{sorted(unknown_support_ids)}"
        )


def _require_fields(record: dict, required: set[str], label: str) -> None:
    missing = sorted(required - set(record))
    if missing:
        raise UnifiedStoryContractError(
            f"{label} is missing required fields: {', '.join(missing)}"
        )
