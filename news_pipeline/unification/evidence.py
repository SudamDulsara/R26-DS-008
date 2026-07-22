import hashlib

from news_pipeline.unification.sentences import (
    is_usable_sentence,
    normalize_sentence,
    split_sentences,
)


def sentence_dedup_key(sentence: str) -> str:
    return normalize_sentence(sentence).casefold()


def build_sentence_evidence(
    article_records: list[dict],
    min_chars: int = 20,
    min_words: int = 3,
    require_sinhala: bool = True,
) -> list[dict]:
    if min_chars < 1:
        raise ValueError("min_chars must be at least 1")
    if min_words < 1:
        raise ValueError("min_words must be at least 1")

    seen_article_ids = set()
    evidence_by_key = {}
    support_by_key = {}

    for article in article_records:
        article_id = article.get("article_id")
        if article_id is None:
            raise ValueError("Every article record must include article_id")
        if article_id in seen_article_ids:
            raise ValueError(f"Duplicate article_id in cluster: {article_id}")
        seen_article_ids.add(article_id)

        sentences = split_sentences(article.get("clean_text") or "")
        for sentence_index, sentence in enumerate(sentences):
            if not is_usable_sentence(
                sentence,
                min_chars=min_chars,
                min_words=min_words,
                require_sinhala=require_sinhala,
            ):
                continue
            dedup_key = sentence_dedup_key(sentence)
            evidence = evidence_by_key.get(dedup_key)
            if evidence is None:
                evidence = {
                    "sentence_id": _sentence_id(dedup_key),
                    "text": sentence,
                    "support_count": 0,
                    "occurrence_count": 0,
                    "supporting_articles": [],
                }
                evidence_by_key[dedup_key] = evidence
                support_by_key[dedup_key] = {}

            article_support = support_by_key[dedup_key].get(article_id)
            if article_support is None:
                article_support = {
                    "article_id": article_id,
                    "source": article.get("source"),
                    "url": article.get("url"),
                    "title": article.get("title"),
                    "published_date": article.get("published_date"),
                    "is_representative": bool(article.get("is_representative")),
                    "sentence_indexes": [],
                }
                support_by_key[dedup_key][article_id] = article_support
                evidence["supporting_articles"].append(article_support)
                evidence["support_count"] += 1

            article_support["sentence_indexes"].append(sentence_index)
            evidence["occurrence_count"] += 1

    return list(evidence_by_key.values())


def _sentence_id(dedup_key: str) -> str:
    digest = hashlib.sha256(dedup_key.encode("utf-8")).hexdigest()[:24]
    return f"sentence_{digest}"
