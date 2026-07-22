from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from news_pipeline.clustering.text import ClusterArticle, lexical_overlap


@dataclass(frozen=True)
class CandidatePair:
    left_id: int
    right_id: int
    lexical_overlap: float
    hours_apart: Optional[float]


def build_candidate_pairs(
    articles: list[ClusterArticle],
    window_hours: int,
    allow_same_source_pairs: bool,
    min_lexical_overlap: float,
) -> list[CandidatePair]:
    candidates: list[CandidatePair] = []
    sorted_articles = sorted(
        articles,
        key=lambda article: article.event_time or datetime.max,
    )

    for left_index, left in enumerate(sorted_articles):
        for right in sorted_articles[left_index + 1 :]:
            hours_apart = _hours_between(left.event_time, right.event_time)
            if hours_apart is not None and hours_apart > window_hours:
                break

            if not allow_same_source_pairs and left.source == right.source:
                continue

            overlap = lexical_overlap(left.similarity_text, right.similarity_text)
            if overlap < min_lexical_overlap:
                continue

            candidates.append(
                CandidatePair(
                    left_id=left.id,
                    right_id=right.id,
                    lexical_overlap=overlap,
                    hours_apart=hours_apart,
                )
            )

    return candidates


def _hours_between(left_time, right_time) -> Optional[float]:
    if left_time is None or right_time is None:
        return None
    return abs((right_time - left_time).total_seconds()) / 3600
