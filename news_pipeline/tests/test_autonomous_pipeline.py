import csv
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from news_pipeline.clustering.candidate_builder import build_candidate_pairs
from news_pipeline.clustering.event_clusterer import (
    ScoredPair,
    _prevent_incompatible_components,
    _prevent_bridge_merges,
    run_event_clustering,
)
from news_pipeline.clustering.semantic_partition import (
    validate_semantic_partition,
)
from news_pipeline.clustering.semantic_constraints import (
    load_active_different_event_pairs,
)
from news_pipeline.clustering.text import ClusterArticle
from news_pipeline.cleaner.sinhala_cleaner import (
    assess_article_quality,
    clean_article,
    is_structurally_unsupported_media,
    run_cleaner,
)
from news_pipeline.crawler.rss_crawler import (
    _ada_catchup,
    _discover_source,
    build_feed_page_urls,
)
from news_pipeline.extractor.article_extractor import (
    _best_extraction,
    _summarize_extraction_origins,
)
from news_pipeline.config import load_config
from news_pipeline.observability import (
    _stage_counts,
    write_pipeline_health_report,
)
from news_pipeline.storage.database import get_connection, initialize_db
from news_pipeline.unification.autonomous_audit import (
    AutonomousAuditResponse,
    classify_audit_route,
    decide_autonomous_audit,
)
from news_pipeline.unification.final_publication import (
    _article_dispositions,
    materialize_gpt_only_publication,
)
from news_pipeline.unification.gpt_contract import (
    GPTClaimV2,
    GPTUnifiedStoryResponseV2,
    GPT_OUTPUT_SCHEMA_VERSION_V2,
    UNTRUSTED_SOURCE_DATA_BEGIN,
    UNTRUSTED_SOURCE_DATA_END,
)
from news_pipeline.unification.gpt_preflight import (
    MODEL_PRICING,
    OfflineRequestSizePreflight,
)
from news_pipeline.unification.openai_adapter import (
    AdapterOutcome,
    AdapterResult,
    StructuredResponseRequest,
)
from news_pipeline.unification.production import (
    _audit_circuit_breaker_status,
    _budget_safe_audit_route,
    _cached_candidate_requires_autonomous_audit,
    run_gpt_unification,
    version_is_deployable_gpt,
)


class _TwoStageGenerator:
    def __init__(self, *, partition_once=False):
        self.requests = []
        self.partition_once = partition_once

    def generate(self, request):
        self.requests.append(request)
        if UNTRUSTED_SOURCE_DATA_BEGIN in request.input:
            source_json = request.input.split(
                f"{UNTRUSTED_SOURCE_DATA_BEGIN}\n", 1
            )[1].split(f"\n{UNTRUSTED_SOURCE_DATA_END}", 1)[0]
            source = json.loads(source_json)
        else:
            source = json.loads(request.input.split("\n", 1)[1])["source"]
        evidence_ids = [
            article["evidence_spans"][0]["evidence_span_id"]
            for article in source["articles"]
        ]
        output = GPTUnifiedStoryResponseV2(
            schema_version=GPT_OUTPUT_SCHEMA_VERSION_V2,
            display_title="Verified event update",
            unified_story="Verified source reports describe the event.",
            claims=[
                GPTClaimV2(
                    claim_text="Verified source reports describe the event.",
                    evidence_span_ids=evidence_ids,
                )
            ],
            conflicts_or_uncertainties=[],
            used_only_supplied_sources=True,
        )
        if request.text_format is AutonomousAuditResponse:
            if self.partition_once and len(source["articles"]) == 3:
                self.partition_once = False
                output_text = AutonomousAuditResponse(
                    cluster_coherence="partition_required",
                    article_groups=[[1, 2], [3]],
                    corrected_story=None,
                    change_level="material",
                    correction_categories=["source_coverage"],
                ).model_dump_json()
            else:
                output_text = AutonomousAuditResponse(
                    cluster_coherence="coherent",
                    article_groups=[],
                    corrected_story=output,
                    change_level="none",
                    correction_categories=[],
                ).model_dump_json()
        else:
            output_text = output.model_dump_json()
        response = SimpleNamespace(
            id=f"response-{len(self.requests)}",
            model=request.model,
            status="completed",
            output_parsed=None,
            output_text=output_text,
            usage=SimpleNamespace(
                input_tokens=100,
                output_tokens=30,
                total_tokens=130,
            ),
        )
        return AdapterResult(
            outcome=AdapterOutcome.SUCCESS,
            response=response,
            attempts=1,
        )


class _CountingEmbedder:
    def __init__(self, model_name="counting", model_revision="revision-a"):
        self.model_name = model_name
        self.model_revision = model_revision
        self.encoded_texts = []

    def encode(self, texts, batch_size=16):
        self.encoded_texts.extend(texts)
        return [
            [1.0, 0.0] if "quake" in text else [0.0, 1.0]
            for text in texts
        ]


class _RejectTerraPreflight(OfflineRequestSizePreflight):
    def fit_request_to_budget(self, request, *, minimum_output_tokens=1):
        if request.model == "gpt-5.6-terra":
            return None
        return super().fit_request_to_budget(
            request,
            minimum_output_tokens=minimum_output_tokens,
        )


class AutonomousPipelineTests(unittest.TestCase):
    def _prepare_cluster(self, root):
        config = replace(
            load_config(load_env_file=False),
            data_dir=root,
            db_path=root / "pipeline.db",
            gpt_enabled=True,
            gpt_only_publication_enabled=True,
            gpt_autonomous_audit_enabled=True,
        )
        with patch(
            "news_pipeline.storage.database.load_config",
            return_value=config,
        ):
            initialize_db()
        connection = get_connection(config)
        connection.executemany(
            """
            INSERT INTO articles (
                id, url, source, title, published_date, clean_text,
                clean_status, dedupe_status, crawl_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, 'cleaned', 'unique', ?)
            """,
            [
                (
                    1,
                    "https://example.test/one",
                    "Publisher A",
                    "First report",
                    "2026-08-13T08:00:00+05:30",
                    "The first verified source report.",
                    "2026-08-13T08:05:00+05:30",
                ),
                (
                    2,
                    "https://example.test/two",
                    "Publisher B",
                    "Second report",
                    "2026-08-13T09:00:00+05:30",
                    "The second verified source report.",
                    "2026-08-13T09:05:00+05:30",
                ),
            ],
        )
        connection.executemany(
            """
            INSERT INTO discovered_urls (
                url, source, status, fetched, discovery_method
            ) VALUES (?, ?, 'extracted', 1, 'test')
            """,
            [
                ("https://example.test/one", "Publisher A"),
                ("https://example.test/two", "Publisher B"),
            ],
        )
        connection.execute(
            """
            INSERT INTO story_clusters (
                id, cluster_key, representative_article_id, model_name,
                model_revision, text_variant, similarity_threshold,
                representative_threshold, cohesion_threshold,
                event_date_start, event_date_end, article_count,
                source_count, confidence
            ) VALUES (
                1, 'story-test', 1, 'hashing', 'builtin-v1',
                'title_lead', 0.85, 0.90, 0.85,
                '2026-08-13', '2026-08-13', 2, 2, 0.95
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO story_cluster_members (
                cluster_id, article_id, similarity_score,
                is_representative
            ) VALUES (1, ?, ?, ?)
            """,
            [(1, 1.0, 1), (2, 0.94, 0)],
        )
        connection.commit()
        connection.close()
        return config

    def test_validator_warning_requires_and_accepts_model_audit(self):
        candidate = {
            "generation_status": "accepted",
            "validation_status": "accepted_with_warnings",
            "response_id": "response-1",
            "resolved_output_json": "{}",
        }
        self.assertFalse(version_is_deployable_gpt(candidate))
        self.assertTrue(
            version_is_deployable_gpt(
                {**candidate, "autonomous_audit_status": "accepted"}
            )
        )

    def test_normal_three_source_cluster_uses_medium_luna_route(self):
        config = load_config(load_env_file=False)
        route = classify_audit_route(
            cluster={"article_count": 3, "source_count": 3},
            members=[
                {"source": "A", "clean_text": "one"},
                {"source": "B", "clean_text": "two"},
                {"source": "C", "clean_text": "three"},
            ],
            config=config,
        )
        self.assertEqual(route.risk_tier, "medium")
        self.assertEqual(route.complexity, "standard")
        self.assertEqual(route.model, config.gpt_audit_model)

    def test_single_publisher_cluster_uses_medium_reasoning_luna(self):
        config = load_config(load_env_file=False)
        route = classify_audit_route(
            cluster={"article_count": 2, "source_count": 1},
            members=[
                {"source": "A", "clean_text": "one"},
                {"source": "A", "clean_text": "two"},
            ],
            config=config,
        )
        self.assertEqual(route.risk_tier, "medium")
        self.assertEqual(route.model, config.gpt_audit_model)
        self.assertEqual(
            route.reasoning_effort,
            config.gpt_audit_complex_reasoning_effort,
        )
        self.assertIn("single_publisher_semantic_risk", route.reasons)

    def test_large_structural_cluster_uses_high_risk_terra_route(self):
        config = load_config(load_env_file=False)
        route = classify_audit_route(
            cluster={"article_count": 8, "source_count": 5},
            members=[
                {"source": "A", "clean_text": "one"},
                {"source": "B", "clean_text": "two"},
            ],
            config=config,
        )
        self.assertEqual(route.risk_tier, "high")
        self.assertEqual(route.complexity, "complex")
        self.assertEqual(route.model, config.gpt_audit_complex_model)

    def test_complex_audit_has_a_budget_safe_luna_route(self):
        config = load_config(load_env_file=False)
        preferred = classify_audit_route(
            cluster={"article_count": 8, "source_count": 5},
            members=[
                {"source": "A", "clean_text": "one"},
                {"source": "B", "clean_text": "two"},
                {"source": "C", "clean_text": "three"},
            ],
            config=config,
        )
        fallback = _budget_safe_audit_route(preferred, config=config)
        self.assertIsNotNone(fallback)
        self.assertEqual(fallback.model, config.gpt_audit_model)
        self.assertEqual(
            fallback.reasoning_effort,
            config.gpt_audit_reasoning_effort,
        )
        self.assertIn("budget_safe_luna_route", fallback.reasons)

    def test_shadow_policy_measures_clean_low_risk_skip_but_still_audits(self):
        config = replace(
            load_config(load_env_file=False),
            gpt_audit_policy_mode="shadow",
            gpt_low_risk_audit_sample_rate=0.0,
        )
        route = classify_audit_route(
            cluster={"article_count": 2, "source_count": 2},
            members=[
                {"source": "A", "clean_text": "one"},
                {"source": "B", "clean_text": "two"},
            ],
            config=config,
        )
        decision = decide_autonomous_audit(
            route=route,
            primary={
                "generation_status": "accepted",
                "validation_status": "accepted",
                "response_id": "response-1",
                "output_json": "{}",
                "resolved_output_json": "{}",
            },
            request_fingerprint_sha256="fingerprint",
            config=config,
        )
        self.assertTrue(decision.should_audit)
        self.assertFalse(decision.would_audit_under_risk_policy)

    def test_risk_policy_skips_only_mechanically_clean_low_risk_candidate(self):
        config = replace(
            load_config(load_env_file=False),
            gpt_audit_policy_mode="risk_tiered",
            gpt_low_risk_audit_sample_rate=0.0,
        )
        route = classify_audit_route(
            cluster={"article_count": 2, "source_count": 2},
            members=[
                {"source": "A", "clean_text": "one"},
                {"source": "B", "clean_text": "two"},
            ],
            config=config,
        )
        clean = {
            "generation_status": "accepted",
            "validation_status": "accepted",
            "response_id": "response-1",
            "output_json": "{}",
            "resolved_output_json": "{}",
        }
        skipped = decide_autonomous_audit(
            route=route,
            primary=clean,
            request_fingerprint_sha256="fingerprint",
            config=config,
        )
        self.assertFalse(skipped.should_audit)
        warning = decide_autonomous_audit(
            route=route,
            primary={**clean, "validation_status": "accepted_with_warnings"},
            request_fingerprint_sha256="fingerprint",
            config=config,
        )
        self.assertTrue(warning.should_audit)

    def test_budget_interrupted_cached_audit_is_retryable(self):
        config = replace(
            load_config(load_env_file=False),
            gpt_autonomous_audit_enabled=True,
        )
        candidate = {
            "generation_status": "fallback",
            "validation_status": "autonomous_audit_preflight",
            "response_id": "response-primary",
            "output_json": "{}",
            "resolved_output_json": "{}",
            "autonomous_audit_status": "failed",
        }
        self.assertTrue(
            _cached_candidate_requires_autonomous_audit(
                candidate,
                config=config,
            )
        )
        candidate["validation_status"] = "fact_shape_failed"
        self.assertFalse(
            _cached_candidate_requires_autonomous_audit(
                candidate,
                config=config,
            )
        )

    def test_same_publisher_candidates_are_enabled_by_default(self):
        config = load_config(load_env_file=False)
        self.assertTrue(config.cluster_allow_same_source_pairs)
        self.assertEqual(config.cluster_window_hours, 72)

    def test_zero_lexical_threshold_skips_unused_overlap_work(self):
        articles = [
            ClusterArticle(
                id=1,
                source="Publisher A",
                title="First",
                published_date="",
                crawl_timestamp="",
                clean_text="First body",
                event_time=datetime(2026, 8, 16, 10),
                similarity_text="first body",
            ),
            ClusterArticle(
                id=2,
                source="Publisher B",
                title="Second",
                published_date="",
                crawl_timestamp="",
                clean_text="Second body",
                event_time=datetime(2026, 8, 16, 11),
                similarity_text="second body",
            ),
        ]

        with patch(
            "news_pipeline.clustering.candidate_builder.lexical_overlap",
            side_effect=AssertionError("lexical overlap must not be computed"),
        ):
            pairs = build_candidate_pairs(
                articles,
                window_hours=72,
                allow_same_source_pairs=True,
                min_lexical_overlap=0.0,
            )

        self.assertEqual(len(pairs), 1)
        self.assertEqual((pairs[0].left_id, pairs[0].right_id), (1, 2))
        self.assertEqual(pairs[0].lexical_overlap, 0.0)
        self.assertEqual(pairs[0].hours_apart, 1.0)

    def test_positive_lexical_threshold_retains_overlap_filter(self):
        articles = [
            ClusterArticle(
                id=1,
                source="Publisher A",
                title="First",
                published_date="",
                crawl_timestamp="",
                clean_text="First body",
                event_time=datetime(2026, 8, 16, 10),
                similarity_text="alpha beta",
            ),
            ClusterArticle(
                id=2,
                source="Publisher B",
                title="Second",
                published_date="",
                crawl_timestamp="",
                clean_text="Second body",
                event_time=datetime(2026, 8, 16, 11),
                similarity_text="alpha gamma",
            ),
        ]

        accepted = build_candidate_pairs(
            articles,
            window_hours=72,
            allow_same_source_pairs=True,
            min_lexical_overlap=0.5,
        )
        rejected = build_candidate_pairs(
            articles,
            window_hours=72,
            allow_same_source_pairs=True,
            min_lexical_overlap=0.51,
        )

        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].lexical_overlap, 0.5)
        self.assertEqual(rejected, [])

    def test_committed_settings_hold_non_secret_operational_config(self):
        config = load_config(
            load_env_file=False,
        )
        settings_path = config.project_root / "news_pipeline" / "pipeline_config.json"
        self.assertTrue(settings_path.is_file())
        self.assertFalse((config.project_root / "pipeline_config.json").exists())
        self.assertTrue(config.gpt_enabled)
        self.assertTrue(config.gpt_only_publication_enabled)
        self.assertEqual(config.gpt_audit_policy_mode, "shadow")
        self.assertEqual(config.gpt_low_risk_audit_sample_rate, 0.1)
        self.assertEqual(len(config.news_sources), 9)
        example_lines = (
            config.project_root / ".env.example"
        ).read_text(encoding="utf-8").splitlines()
        self.assertEqual(example_lines, ["OPENAI_API_KEY="])

    def test_singleton_routing_is_not_reported_as_cluster_failure(self):
        counts = _stage_counts(
            "clustering",
            {
                "eligible_articles": 100,
                "affected_articles": 12,
                "story_clusters": 90,
                "representative_unclustered_articles": 7,
                "singleton_articles": 7,
            },
        )
        self.assertEqual(counts["failed"], 0)
        self.assertEqual(counts["skipped"], 88)

    def test_budget_deferral_is_not_reported_as_unification_failure(self):
        counts = _stage_counts(
            "unification",
            {
                "clusters_seen": 420,
                "accepted": 386,
                "cache_hits": 392,
                "fallbacks": 11,
                "budget_deferred": 11,
                "invalid_inputs": 0,
            },
        )
        self.assertEqual(counts["failed"], 0)
        self.assertEqual(counts["skipped"], 403)

    def test_health_report_separates_run_and_publication_outcomes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            stats = {
                "run_metrics": {
                    "status": "completed",
                    "started_at": "2026-08-14T00:00:00+05:30",
                    "finished_at": "2026-08-14T00:01:00+05:30",
                    "duration_seconds": 60,
                    "failed_stage": None,
                    "gpt_usage": {},
                    "stages": {},
                },
                "clustering": {
                    "embedding_cache_enabled": True,
                    "embedding_cache_hits": 610,
                    "embedding_cache_misses": 7,
                    "embedding_encoded_vectors": 7,
                    "embedding_cache_lookup_seconds": 0.02,
                    "embedding_model_load_seconds": 0.4,
                    "embedding_encoding_seconds": 3.5,
                    "embedding_cache_write_seconds": 0.03,
                    "embedding_total_seconds": 3.95,
                    "model_name": "embedding-model",
                    "model_revision": "revision-a",
                },
                "extraction": {
                    "fresh_urls_attempted": 8,
                    "fresh_extracted_articles": 8,
                    "fresh_fetch_failures": 0,
                    "fresh_rejected_articles": 0,
                    "historical_retry_urls_attempted": 1,
                    "historical_retry_extracted_articles": 0,
                    "historical_retry_fetch_failures": 1,
                    "historical_retry_rejected_articles": 0,
                    "historical_retry_failures_by_source": {
                        "Ada Derana Sinhala": {"article_body_incomplete": 1}
                    },
                },
                "unification": {
                    "fallbacks": 2,
                    "fallback_reasons": {"preflight_per_run": 2},
                    "budget_deferred": 2,
                    "cached_fallbacks": 17,
                    "cached_fallback_reasons": {"incomplete": 15},
                    "audit_budget_safe_routes": 1,
                },
                "export": {
                    "snapshot_dir": str(output_dir),
                    "final_gpt_only_publication": {
                        "counts": {
                            "final_unified_stories": 4756,
                            "gpt_unified_stories": 403,
                            "singleton_passthrough_stories": 4336,
                            "evidence_safe_fallback_stories": 17,
                            "final_story_sources": 5530,
                            "explicit_nonpublishable_states": 0,
                        },
                        "reconciliation": {
                            "every_cluster_has_one_state": True,
                            "every_discovered_url_has_disposition": True,
                        },
                    },
                },
            }
            paths = write_pipeline_health_report(
                run_id=26,
                stats=stats,
                output_dir=output_dir,
            )
            payload = json.loads(
                Path(paths["json_path"]).read_text(encoding="utf-8")
            )
            markdown = Path(paths["markdown_path"]).read_text(
                encoding="utf-8"
            )
        self.assertEqual(
            payload["unification_outcomes"]["current_run_fallbacks"],
            2,
        )
        self.assertEqual(
            payload["unification_outcomes"]["cached_historical_fallbacks"],
            17,
        )
        self.assertEqual(
            payload["publication"]["counts"]["final_story_sources"],
            5530,
        )
        self.assertEqual(payload["clustering_embedding_cache"]["hits"], 610)
        self.assertEqual(
            payload["clustering_embedding_cache"]["encoded_vectors"],
            7,
        )
        self.assertIn("Cache hits: `610`", markdown)
        self.assertEqual(
            payload["extraction_outcomes"]["fresh"]["fetch_failures"],
            0,
        )
        self.assertEqual(
            payload["extraction_outcomes"]["historical_retries"][
                "fetch_failures"
            ],
            1,
        )
        self.assertIn("Fresh fetch failures: `0`", markdown)
        self.assertIn("Historical retry failures: `1`", markdown)
        self.assertIn("Publication reconciliation passed: `true`", markdown)

    def test_extraction_outcomes_separate_fresh_from_historical_retries(self):
        rows = [
            {
                "source": "Fresh Publisher",
                "status": "extracted",
                "last_error_code": None,
                "discovered_at": "2026-08-16T18:23:44",
            },
            {
                "source": "Fresh Publisher",
                "status": "fetch_failed",
                "last_error_code": "network_error",
                "discovered_at": "2026-08-16T18:23:44",
            },
            {
                "source": "Archive Publisher",
                "status": "exhausted",
                "last_error_code": "article_body_incomplete",
                "discovered_at": "2026-08-14T04:47:08",
            },
        ]
        result = _summarize_extraction_origins(
            rows,
            fresh_discovered_at="2026-08-16T18:23:44",
        )
        self.assertEqual(result["fresh_urls_attempted"], 2)
        self.assertEqual(result["fresh_extracted_articles"], 1)
        self.assertEqual(result["fresh_fetch_failures"], 1)
        self.assertEqual(result["historical_retry_urls_attempted"], 1)
        self.assertEqual(result["historical_retry_fetch_failures"], 1)
        self.assertEqual(
            result["historical_retry_failures_by_source"],
            {"Archive Publisher": {"article_body_incomplete": 1}},
        )

    def test_cartoon_url_route_is_explicit_unsupported_media(self):
        url = "https://publisher.test/cartoons/12345"
        self.assertTrue(is_structurally_unsupported_media(url))
        status, flags = assess_article_quality(
            text=("boilerplate navigation text " * 20),
            title="visual item",
            purity=0.0,
            purity_threshold=0.85,
            min_article_length=50,
            url=url,
        )
        self.assertEqual(status, "unsupported_media")
        self.assertEqual(flags, ["unsupported_image_only_media"])
        self.assertFalse(
            is_structurally_unsupported_media(
                "https://publisher.test/news/cartoonist-interview"
            )
        )

    def test_cleaner_reclassifies_existing_cartoon_capture(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = replace(
                load_config(load_env_file=False),
                data_dir=root,
                db_path=root / "pipeline.db",
            )
            with patch(
                "news_pipeline.storage.database.load_config",
                return_value=config,
            ):
                initialize_db()
            connection = get_connection(config)
            url = "https://publisher.test/cartoons/12345"
            connection.execute(
                """
                INSERT INTO discovered_urls (
                    url, source, status, fetched, rss_title
                ) VALUES (?, 'Publisher', 'extracted', 1, 'visual')
                """,
                (url,),
            )
            connection.execute(
                """
                INSERT INTO articles (
                    id, url, source, title, raw_text, clean_text,
                    clean_status, dedupe_status, quality_flags
                ) VALUES (
                    1, ?, 'Publisher', 'visual', ?, ?,
                    'cleaned', 'unique', '[]'
                )
                """,
                (url, "navigation boilerplate " * 20, "navigation boilerplate " * 20),
            )
            connection.commit()
            connection.close()

            with patch(
                "news_pipeline.cleaner.sinhala_cleaner.load_config",
                return_value=config,
            ), patch(
                "news_pipeline.cleaner.sinhala_cleaner.get_connection",
                side_effect=lambda: get_connection(config),
            ):
                result = run_cleaner()

            connection = get_connection(config)
            try:
                row = connection.execute(
                    "SELECT clean_status, quality_flags FROM articles WHERE id = 1"
                ).fetchone()
                disposition = _article_dispositions(connection)[0]
            finally:
                connection.close()

        self.assertEqual(result["unsupported_media_articles"], 1)
        self.assertEqual(row["clean_status"], "unsupported_media")
        self.assertEqual(
            json.loads(row["quality_flags"]),
            ["unsupported_image_only_media"],
        )
        self.assertEqual(disposition["disposition"], "unsupported_media")
        self.assertEqual(
            json.loads(disposition["reason_codes_json"]),
            ["unsupported_image_only_media"],
        )

    def test_embedding_cache_preserves_uncached_memberships_and_scores(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = replace(
                load_config(load_env_file=False),
                data_dir=root,
                db_path=root / "pipeline.db",
                cluster_model_name="counting",
                cluster_model_revision="revision-a",
                cluster_similarity_threshold=0.9,
                cluster_representative_threshold=0.9,
                cluster_cohesion_threshold=0.9,
            )
            with patch(
                "news_pipeline.storage.database.load_config",
                return_value=config,
            ):
                initialize_db()
            connection = get_connection(config)
            connection.executemany(
                """
                INSERT INTO articles (
                    id, url, source, title, published_date, clean_text,
                    clean_status, dedupe_status, crawl_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, 'cleaned', 'unique', ?)
                """,
                [
                    (
                        1,
                        "https://example.test/1",
                        "Source A",
                        "quake report one",
                        "2026-08-16T10:00:00",
                        "quake details",
                        "2026-08-16T10:01:00",
                    ),
                    (
                        2,
                        "https://example.test/2",
                        "Source B",
                        "quake report two",
                        "2026-08-16T10:05:00",
                        "quake details",
                        "2026-08-16T10:06:00",
                    ),
                    (
                        3,
                        "https://example.test/3",
                        "Source C",
                        "sports report",
                        "2026-08-16T10:10:00",
                        "sports details",
                        "2026-08-16T10:11:00",
                    ),
                ],
            )
            connection.commit()
            connection.close()

            def memberships_and_scores():
                current = get_connection(config)
                try:
                    return [
                        (
                            str(row["cluster_key"]),
                            int(row["article_id"]),
                            float(row["similarity_score"]),
                        )
                        for row in current.execute(
                            """
                            SELECT clusters.cluster_key, members.article_id,
                                   members.similarity_score
                            FROM story_clusters AS clusters
                            JOIN story_cluster_members AS members
                              ON members.cluster_id = clusters.id
                            ORDER BY clusters.cluster_key, members.article_id
                            """
                        )
                    ]
                finally:
                    current.close()

            created_embedders = []

            def create_counting(model_name, model_revision):
                embedder = _CountingEmbedder(model_name, model_revision)
                created_embedders.append(embedder)
                return embedder

            patch_config = patch(
                "news_pipeline.clustering.event_clusterer.load_config",
                return_value=config,
            )
            patch_embedder = patch(
                "news_pipeline.clustering.event_clusterer.create_embedder",
                side_effect=create_counting,
            )
            patch_connection = patch(
                "news_pipeline.clustering.event_clusterer.get_connection",
                side_effect=lambda: get_connection(config),
            )
            with patch_config, patch_embedder, patch_connection:
                uncached = run_event_clustering(use_embedding_cache=False)
                uncached_memberships = memberships_and_scores()
                cold = run_event_clustering(
                    force_article_ids={1, 2, 3},
                    use_embedding_cache=True,
                )
                cold_memberships = memberships_and_scores()

            with patch(
                "news_pipeline.clustering.event_clusterer.load_config",
                return_value=config,
            ), patch(
                "news_pipeline.clustering.event_clusterer.create_embedder",
                side_effect=AssertionError("warm cache loaded the model"),
            ), patch(
                "news_pipeline.clustering.event_clusterer.get_connection",
                side_effect=lambda: get_connection(config),
            ):
                warm = run_event_clustering(
                    force_article_ids={1, 2, 3},
                    use_embedding_cache=True,
                )
                warm_memberships = memberships_and_scores()

            changed_connection = get_connection(config)
            changed_connection.execute(
                "UPDATE articles SET title = ? WHERE id = 3",
                ("sports report updated",),
            )
            changed_connection.commit()
            changed_connection.close()
            with patch(
                "news_pipeline.clustering.event_clusterer.load_config",
                return_value=config,
            ), patch(
                "news_pipeline.clustering.event_clusterer.create_embedder",
                side_effect=create_counting,
            ), patch(
                "news_pipeline.clustering.event_clusterer.get_connection",
                side_effect=lambda: get_connection(config),
            ):
                changed_input = run_event_clustering(
                    force_article_ids={1, 2, 3},
                    use_embedding_cache=True,
                )

            revision_b_config = replace(
                config,
                cluster_model_revision="revision-b",
            )
            with patch(
                "news_pipeline.clustering.event_clusterer.load_config",
                return_value=revision_b_config,
            ), patch(
                "news_pipeline.clustering.event_clusterer.create_embedder",
                side_effect=create_counting,
            ), patch(
                "news_pipeline.clustering.event_clusterer.get_connection",
                side_effect=lambda: get_connection(revision_b_config),
            ):
                changed_revision = run_event_clustering(
                    force_article_ids={1, 2, 3},
                    use_embedding_cache=True,
                )

            cache_connection = get_connection(config)
            try:
                cache_rows = cache_connection.execute(
                    "SELECT COUNT(*) AS count FROM clustering_embedding_cache"
                ).fetchone()["count"]
            finally:
                cache_connection.close()

        self.assertEqual(uncached["embedding_cache_hits"], 0)
        self.assertEqual(uncached["embedding_cache_misses"], 3)
        self.assertEqual(uncached["embedding_encoded_vectors"], 3)
        self.assertEqual(cold["embedding_cache_hits"], 0)
        self.assertEqual(cold["embedding_cache_misses"], 3)
        self.assertEqual(cold["embedding_encoded_vectors"], 3)
        self.assertEqual(warm["embedding_cache_hits"], 3)
        self.assertEqual(warm["embedding_cache_misses"], 0)
        self.assertEqual(warm["embedding_encoded_vectors"], 0)
        self.assertEqual(changed_input["embedding_cache_hits"], 2)
        self.assertEqual(changed_input["embedding_cache_misses"], 1)
        self.assertEqual(changed_input["embedding_encoded_vectors"], 1)
        self.assertEqual(changed_revision["embedding_cache_hits"], 0)
        self.assertEqual(changed_revision["embedding_cache_misses"], 3)
        self.assertEqual(changed_revision["embedding_encoded_vectors"], 3)
        self.assertEqual(len(created_embedders), 4)
        self.assertEqual(cache_rows, 7)
        self.assertEqual(uncached_memberships, cold_memberships)
        self.assertEqual(cold_memberships, warm_memberships)

    def test_bridge_guard_keeps_only_the_strongest_old_story(self):
        pairs = [
            ScoredPair(9, 1, 0.97, 0.5, 1.0),
            ScoredPair(9, 2, 0.95, 0.5, 1.0),
        ]
        retained, removed = _prevent_bridge_merges(
            linked_pairs=pairs,
            changed_article_ids={9},
            clusters_by_key={
                "old-a": {"representative_article_id": 1},
                "old-b": {"representative_article_id": 2},
            },
            cluster_key_by_article={1: "old-a", 2: "old-b"},
            embeddings={
                1: [1.0, 0.0],
                2: [0.0, 1.0],
                9: [0.8, 0.6],
            },
            representative_threshold=0.9,
        )
        self.assertEqual(removed, 1)
        self.assertEqual([(pair.left_id, pair.right_id) for pair in retained], [(9, 1)])

    def test_semantic_constraints_prevent_indirect_component_bridge(self):
        pairs = [
            ScoredPair(1, 9, 0.98, 0.5, 1.0),
            ScoredPair(9, 2, 0.97, 0.5, 1.0),
            ScoredPair(2, 8, 0.96, 0.5, 1.0),
        ]
        retained, removed = _prevent_incompatible_components(
            linked_pairs=pairs,
            incompatible_pairs={(1, 2)},
        )
        self.assertEqual(removed, 1)
        self.assertEqual(
            {(pair.left_id, pair.right_id) for pair in retained},
            {(1, 9), (2, 8)},
        )

    def test_semantic_constraints_keep_compatible_multi_article_groups(self):
        pairs = [
            ScoredPair(1, 9, 0.98, 0.5, 1.0),
            ScoredPair(2, 8, 0.97, 0.5, 1.0),
        ]
        retained, removed = _prevent_incompatible_components(
            linked_pairs=pairs,
            incompatible_pairs={(1, 2)},
        )
        self.assertEqual(removed, 0)
        self.assertEqual(retained, pairs)

    def test_semantic_partition_preserves_supported_groups_and_singletons(self):
        groups = validate_semantic_partition(
            {1, 2, 3, 4, 5},
            [[3], [5, 4], [2, 1]],
        )
        self.assertEqual(groups, ((1, 2), (3,), (4, 5)))

    def test_semantic_partition_rejects_overlap_or_missing_articles(self):
        with self.assertRaises(ValueError):
            validate_semantic_partition({1, 2, 3}, [[1, 2], [2, 3]])
        with self.assertRaises(ValueError):
            validate_semantic_partition({1, 2, 3}, [[1, 2], [3, 4]])

    def test_mixed_script_sinhala_is_not_rejected_by_purity_alone(self):
        text, purity = clean_article(
            "යුක්රේනය විසින් Novorossiysk වරායට drone ප්රහාරයක් එල්ල කර "
            "තිබේ. ප්රහාරයෙන් grain terminals දෙකකට හානි සිදුවී ඇත. "
            "සිද්ධිය සම්බන්ධයෙන් බලධාරීන් පරීක්ෂණ පවත්වයි."
        )
        status, flags = assess_article_quality(
            text=text,
            title="වරායට ප්රහාරයක්",
            purity=purity,
            purity_threshold=0.95,
            min_article_length=50,
        )
        self.assertEqual(status, "cleaned")
        self.assertIn("low_sinhala_purity_advisory", flags)

    def test_navigation_only_extraction_is_retryable(self):
        html = """
        <html><body><main class='main-content'>
        <h1>පුවත් සිරස්තලය</h1><div>Toggle navigation</div><div>Home</div>
        <div>Archive</div><div>Contact us</div><div>January 1, 1970</div>
        </main></body></html>
        """
        data, method, diagnostics = _best_extraction(
            html=html,
            extracted_json=json.dumps({"text": "Toggle navigation\nHome\nArchive\nContact us"}),
            rss_title="පුවත් සිරස්තලය",
            rss_summary="",
            min_length=50,
        )
        self.assertIsNone(data)
        self.assertIsNone(method)
        self.assertTrue(diagnostics)

    def test_wordpress_feed_pages_are_bounded_and_deterministic(self):
        self.assertEqual(
            build_feed_page_urls("https://publisher.test/feed", 3),
            [
                "https://publisher.test/feed",
                "https://publisher.test/feed?paged=2",
                "https://publisher.test/feed?paged=3",
            ],
        )

    def test_autonomous_defaults_cover_checkpoint_backlog(self):
        config = load_config(load_env_file=False)
        self.assertEqual(config.discovery_max_pages, 50)
        self.assertEqual(config.extraction_workers, 4)
        self.assertEqual(config.gpt_max_clusters_per_run, 100)

    def test_repeated_wordpress_page_finishes_available_feed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = replace(
                load_config(load_env_file=False),
                data_dir=root,
                db_path=root / "pipeline.db",
            )
            with patch(
                "news_pipeline.storage.database.load_config",
                return_value=config,
            ):
                initialize_db()
            connection = get_connection(config)
            run_started = datetime(2026, 8, 14, 3, 0, 0)
            entry = {
                "url": "https://publisher.test/latest",
                "title": "Latest report",
                "published": run_started.isoformat(),
                "summary": "",
            }
            try:
                with patch(
                    "news_pipeline.crawler.rss_crawler._fetch_rss_page",
                    side_effect=[([entry], None), ([entry], None)],
                ):
                    result = _discover_source(
                        source="Publisher",
                        rss_url="https://publisher.test/feed",
                        connection=connection,
                        run_started=run_started,
                        overlap_hours=2,
                        max_pages=50,
                        max_catchup_days=31,
                    )
            finally:
                connection.close()
        self.assertEqual(result["status"], "coverage_complete_with_catchup")
        self.assertEqual(
            result["stop_reason"],
            "feed_pagination_repeated_page",
        )
        self.assertEqual(result["feed_pages_fetched"], 2)
        self.assertEqual(result["new"], 1)

    def test_empty_current_ada_archive_day_is_complete(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = replace(
                load_config(load_env_file=False),
                data_dir=root,
                db_path=root / "pipeline.db",
            )
            with patch(
                "news_pipeline.storage.database.load_config",
                return_value=config,
            ):
                initialize_db()
            connection = get_connection(config)
            run_started = datetime(2026, 8, 14, 3, 0, 0)
            try:
                with patch(
                    "news_pipeline.crawler.rss_crawler._ada_archive_entries",
                    return_value=([], "archive_empty"),
                ):
                    totals, error, requests = _ada_catchup(
                        connection=connection,
                        boundary=run_started - timedelta(hours=2),
                        run_started=run_started,
                        max_days=31,
                    )
            finally:
                connection.close()
        self.assertIsNone(error)
        self.assertEqual(requests, 1)
        self.assertEqual(totals["entries"], 0)

    def test_current_gpt_56_prices_match_official_model_pages(self):
        luna = MODEL_PRICING["gpt-5.6-luna"]
        terra = MODEL_PRICING["gpt-5.6-terra"]
        self.assertEqual(luna.input_usd_per_million_tokens, Decimal("0.20"))
        self.assertEqual(luna.output_usd_per_million_tokens, Decimal("1.20"))
        self.assertEqual(terra.input_usd_per_million_tokens, Decimal("2.00"))
        self.assertEqual(terra.output_usd_per_million_tokens, Decimal("12.00"))

    def test_offline_preflight_fits_and_releases_capacity(self):
        preflight = OfflineRequestSizePreflight(
            max_cost_per_story_usd=Decimal("0.005"),
            max_cost_per_run_usd=Decimal("0.005"),
            provider_framing_token_allowance=0,
            text_format_converter=lambda _format: {},
        )
        request = StructuredResponseRequest(
            model="gpt-5.6-luna",
            instructions="Return a structured result.",
            input="x" * 100,
            text_format=GPTUnifiedStoryResponseV2,
            max_output_tokens=8192,
            reasoning_effort="none",
        )
        fitted = preflight.fit_request_to_budget(
            request,
            minimum_output_tokens=1024,
        )
        self.assertIsNotNone(fitted)
        self.assertLess(fitted.max_output_tokens, request.max_output_tokens)
        report = preflight.evaluate(fitted)
        self.assertTrue(report.should_generate)
        self.assertGreater(preflight.run_reserved_cost_usd, 0)
        preflight.release(report)
        self.assertEqual(preflight.run_reserved_cost_usd, 0)

    def test_atomic_budget_defers_before_primary_provider_call(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._prepare_cluster(Path(temp_dir))
            generator = _TwoStageGenerator()
            preflight = OfflineRequestSizePreflight(
                max_cost_per_story_usd=Decimal("0.0001"),
                max_cost_per_run_usd=Decimal("0.0001"),
            )
            result = run_gpt_unification(
                config=config,
                cluster_keys=["story-test"],
                generator=generator,
                preflight=preflight,
            )
        self.assertEqual(result["atomic_budget_deferred"], 1, result)
        self.assertEqual(result["provider_calls"], 0)
        self.assertEqual(generator.requests, [])

    def test_primary_candidate_is_followed_by_autonomous_audit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._prepare_cluster(Path(temp_dir))
            generator = _TwoStageGenerator()
            preflight = OfflineRequestSizePreflight(
                max_cost_per_story_usd=100,
                max_cost_per_run_usd=100,
            )
            result = run_gpt_unification(
                config=config,
                cluster_keys=["story-test"],
                generator=generator,
                preflight=preflight,
            )
        self.assertEqual(result["generation_calls"], 1, result)
        self.assertEqual(result["audit_calls"], 1)
        self.assertEqual(result["provider_calls"], 2)
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(len(generator.requests), 2)
        self.assertLess(float(preflight.run_reserved_cost_usd), 0.01)

    def test_audit_partition_regenerates_multi_group_in_same_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._prepare_cluster(Path(temp_dir))
            connection = get_connection(config)
            connection.execute(
                """
                INSERT INTO articles (
                    id, url, source, title, published_date, clean_text,
                    clean_status, dedupe_status, crawl_timestamp
                ) VALUES (
                    3, 'https://example.test/three', 'Publisher C',
                    'Unrelated third report', '2026-08-13T10:00:00+05:30',
                    'A separate event with no defensible partner.',
                    'cleaned', 'unique', '2026-08-13T10:05:00+05:30'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO story_cluster_members (
                    cluster_id, article_id, similarity_score,
                    is_representative
                ) VALUES (1, 3, 0.93, 0)
                """
            )
            connection.execute(
                """
                UPDATE story_clusters
                SET article_count = 3, source_count = 3
                WHERE id = 1
                """
            )
            connection.commit()
            connection.close()

            generator = _TwoStageGenerator(partition_once=True)
            preflight = OfflineRequestSizePreflight(
                max_cost_per_story_usd=100,
                max_cost_per_run_usd=100,
            )
            result = run_gpt_unification(
                config=config,
                cluster_keys=["story-test"],
                generator=generator,
                preflight=preflight,
            )
            connection = get_connection(config)
            memberships = {
                tuple(
                    int(member["article_id"])
                    for member in connection.execute(
                        """
                        SELECT article_id
                        FROM story_cluster_members
                        WHERE cluster_id = ?
                        ORDER BY article_id
                        """,
                        (int(cluster["id"]),),
                    )
                )
                for cluster in connection.execute(
                    "SELECT id FROM story_clusters"
                )
            }
            constraint_count = connection.execute(
                "SELECT COUNT(*) AS count FROM semantic_pair_constraints"
            ).fetchone()["count"]
            article_by_id = {
                int(row["id"]): SimpleNamespace(
                    title=row["title"],
                    clean_text=row["clean_text"],
                )
                for row in connection.execute(
                    "SELECT id, title, clean_text FROM articles"
                )
            }
            active_constraints = load_active_different_event_pairs(
                connection.cursor(),
                article_by_id=article_by_id,
            )
            article_by_id[3] = SimpleNamespace(
                title="Changed third report",
                clean_text="Changed source content.",
            )
            expired_constraints = load_active_different_event_pairs(
                connection.cursor(),
                article_by_id=article_by_id,
            )
            connection.close()

        self.assertEqual(result["semantic_partitions_applied"], 1, result)
        self.assertEqual(result["semantic_partition_multi_groups"], 1)
        self.assertEqual(result["semantic_partition_singletons"], 1)
        self.assertEqual(result["generation_calls"], 2)
        self.assertEqual(result["audit_calls"], 2)
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(memberships, {(1, 2), (3,)})
        self.assertEqual(constraint_count, 2)
        self.assertEqual(active_constraints, {(1, 3), (2, 3)})
        self.assertEqual(expired_constraints, set())

    def test_active_risk_policy_skips_clean_low_risk_audit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = replace(
                self._prepare_cluster(Path(temp_dir)),
                gpt_audit_policy_mode="risk_tiered",
                gpt_low_risk_audit_sample_rate=0.0,
            )
            generator = _TwoStageGenerator()
            preflight = OfflineRequestSizePreflight(
                max_cost_per_story_usd=100,
                max_cost_per_run_usd=100,
            )
            with patch(
                "news_pipeline.unification.production."
                "_audit_circuit_breaker_status",
                return_value={
                    "state": "closed_risk_tiered_active",
                    "configured_mode": "risk_tiered",
                    "effective_mode": "risk_tiered",
                },
            ):
                result = run_gpt_unification(
                    config=config,
                    cluster_keys=["story-test"],
                    generator=generator,
                    preflight=preflight,
                )
        self.assertEqual(result["generation_calls"], 1, result)
        self.assertEqual(result["audit_calls"], 0)
        self.assertEqual(result["audits_skipped_low_risk"], 1)
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(len(generator.requests), 1)

    def test_risk_policy_circuit_breaker_requires_model_audit_evidence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = replace(
                self._prepare_cluster(Path(temp_dir)),
                gpt_audit_policy_mode="risk_tiered",
                gpt_audit_circuit_min_evaluated=2,
                gpt_audit_circuit_max_material_rate=0.1,
            )
            connection = get_connection(config)
            try:
                insufficient = _audit_circuit_breaker_status(
                    connection,
                    config=config,
                )
                stats = {
                    "unification": {
                        "audit_change_levels_by_risk": {
                            "low": {"none": 2, "material": 0}
                        }
                    }
                }
                connection.execute(
                    """
                    INSERT INTO pipeline_runs (
                        started_at, finished_at, status, stats_json
                    ) VALUES (?, ?, 'completed', ?)
                    """,
                    (
                        "2026-08-16T10:00:00",
                        "2026-08-16T10:01:00",
                        json.dumps(stats),
                    ),
                )
                connection.commit()
                closed = _audit_circuit_breaker_status(
                    connection,
                    config=config,
                )
            finally:
                connection.close()
        self.assertEqual(
            insufficient["state"],
            "insufficient_evidence_full_audit",
        )
        self.assertEqual(insufficient["effective_mode"], "all")
        self.assertEqual(closed["state"], "closed_risk_tiered_active")
        self.assertEqual(closed["effective_mode"], "risk_tiered")

    def test_shadow_policy_records_model_reported_change_and_avoidable_cost(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = replace(
                self._prepare_cluster(Path(temp_dir)),
                gpt_audit_policy_mode="shadow",
                gpt_low_risk_audit_sample_rate=0.0,
            )
            generator = _TwoStageGenerator()
            preflight = OfflineRequestSizePreflight(
                max_cost_per_story_usd=100,
                max_cost_per_run_usd=100,
            )
            result = run_gpt_unification(
                config=config,
                cluster_keys=["story-test"],
                generator=generator,
                preflight=preflight,
            )
        self.assertEqual(result["audit_calls"], 1, result)
        self.assertEqual(result["audit_policy_would_skip"], 1)
        self.assertEqual(result["shadow_avoidable_audit_calls"], 1)
        self.assertEqual(result["audit_change_levels"], {"none": 1})

    def test_complex_audit_uses_luna_when_terra_cannot_fit_budget(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = replace(
                self._prepare_cluster(Path(temp_dir)),
                gpt_audit_high_risk_article_count=3,
                gpt_audit_high_risk_source_count=3,
            )
            connection = get_connection(config)
            connection.execute(
                """
                INSERT INTO articles (
                    id, url, source, title, published_date, clean_text,
                    clean_status, dedupe_status, crawl_timestamp
                ) VALUES (
                    3, 'https://example.test/three', 'Publisher C',
                    'Third report', '2026-08-13T10:00:00+05:30',
                    'The third verified source report.',
                    'cleaned', 'unique', '2026-08-13T10:05:00+05:30'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO discovered_urls (
                    url, source, status, fetched, discovery_method
                ) VALUES (
                    'https://example.test/three', 'Publisher C',
                    'extracted', 1, 'test'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO story_cluster_members (
                    cluster_id, article_id, similarity_score,
                    is_representative
                ) VALUES (1, 3, 0.93, 0)
                """
            )
            connection.execute(
                """
                UPDATE story_clusters
                SET article_count = 3, source_count = 3
                WHERE id = 1
                """
            )
            connection.commit()
            connection.close()
            generator = _TwoStageGenerator()
            preflight = _RejectTerraPreflight(
                max_cost_per_story_usd=100,
                max_cost_per_run_usd=100,
            )
            result = run_gpt_unification(
                config=config,
                cluster_keys=["story-test"],
                generator=generator,
                preflight=preflight,
            )
        self.assertEqual(result["accepted"], 1, result)
        self.assertEqual(result["audit_budget_safe_routes"], 1)
        self.assertEqual(len(generator.requests), 2)
        self.assertEqual(generator.requests[1].model, config.gpt_audit_model)

    def test_missing_gpt_candidate_publishes_all_cluster_sources(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self._prepare_cluster(root)

            output_dir = root / "publication"
            manifest = materialize_gpt_only_publication(
                output_dir=output_dir,
                config=config,
            )

            self.assertEqual(manifest["counts"]["final_unified_stories"], 1)
            self.assertEqual(manifest["counts"]["final_story_sources"], 2)
            self.assertEqual(
                manifest["counts"]["article_dispositions"]["published"],
                2,
            )
            self.assertEqual(manifest["counts"]["evidence_safe_fallback_stories"], 1)
            self.assertEqual(manifest["counts"]["explicit_nonpublishable_states"], 0)
            with (output_dir / "final_story_sources.csv").open(
                encoding="utf-8-sig", newline=""
            ) as handle:
                source_rows = list(csv.DictReader(handle))
            self.assertEqual(
                {int(row["article_id"]) for row in source_rows},
                {1, 2},
            )
            with (
                output_dir / "final_story_publication_states.csv"
            ).open(encoding="utf-8-sig", newline="") as handle:
                state_rows = list(csv.DictReader(handle))
            self.assertIn(
                "evidence_safe_fallback",
                json.loads(state_rows[0]["reason_codes_json"]),
            )


if __name__ == "__main__":
    unittest.main()
