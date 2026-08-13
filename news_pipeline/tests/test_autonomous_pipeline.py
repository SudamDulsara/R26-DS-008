import csv
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from news_pipeline.clustering.event_clusterer import (
    ScoredPair,
    _prevent_bridge_merges,
)
from news_pipeline.config import load_config
from news_pipeline.observability import _stage_counts
from news_pipeline.storage.database import get_connection, initialize_db
from news_pipeline.unification.autonomous_audit import classify_audit_route
from news_pipeline.unification.final_publication import (
    materialize_gpt_only_publication,
)
from news_pipeline.unification.gpt_contract import (
    GPTClaimV2,
    GPTUnifiedStoryResponseV2,
    GPT_OUTPUT_SCHEMA_VERSION_V2,
    UNTRUSTED_SOURCE_DATA_BEGIN,
    UNTRUSTED_SOURCE_DATA_END,
)
from news_pipeline.unification.gpt_preflight import OfflineRequestSizePreflight
from news_pipeline.unification.openai_adapter import (
    AdapterOutcome,
    AdapterResult,
)
from news_pipeline.unification.production import (
    run_gpt_unification,
    version_is_deployable_gpt,
)


class _TwoStageGenerator:
    def __init__(self):
        self.requests = []

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
        response = SimpleNamespace(
            id=f"response-{len(self.requests)}",
            model=request.model,
            status="completed",
            output_parsed=None,
            output_text=output.model_dump_json(),
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

    def test_complex_clusters_use_the_complex_audit_route(self):
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
        self.assertEqual(route.complexity, "complex")
        self.assertEqual(route.model, config.gpt_audit_complex_model)

    def test_same_publisher_candidates_are_enabled_by_default(self):
        config = load_config(load_env_file=False)
        self.assertTrue(config.cluster_allow_same_source_pairs)
        self.assertEqual(config.cluster_window_hours, 72)

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

    def test_primary_candidate_is_followed_by_autonomous_audit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._prepare_cluster(Path(temp_dir))
            generator = _TwoStageGenerator()
            result = run_gpt_unification(
                config=config,
                cluster_keys=["story-test"],
                generator=generator,
                preflight=OfflineRequestSizePreflight(
                    max_cost_per_story_usd=100,
                    max_cost_per_run_usd=100,
                ),
            )
        self.assertEqual(result["generation_calls"], 1, result)
        self.assertEqual(result["audit_calls"], 1)
        self.assertEqual(result["provider_calls"], 2)
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(len(generator.requests), 2)

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
