import hashlib
import math
from typing import Optional, Protocol

from news_pipeline.clustering.text import tokenize_for_similarity


class TextEmbedder(Protocol):
    model_name: str
    model_revision: str

    def encode(self, texts: list[str], batch_size: int) -> list[list[float]]:
        ...


class HashingEmbedder:
    """Small deterministic embedder for tests and offline smoke runs."""

    def __init__(self, dimensions: int = 384):
        self.model_name = "hashing"
        self.model_revision = "builtin-v1"
        self.dimensions = dimensions

    def encode(self, texts: list[str], batch_size: int = 32) -> list[list[float]]:
        return [_normalize(_hash_text(text, self.dimensions)) for text in texts]


class SentenceTransformerEmbedder:
    def __init__(self, model_name: str, model_revision: Optional[str] = None):
        self.model_name = model_name
        _enable_system_cert_store()
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers is required for embedding-based clustering. "
                "Install requirements.txt, or run with --model hashing for a "
                "lightweight offline smoke test."
            ) from exc

        try:
            self._model = SentenceTransformer(
                model_name,
                revision=model_revision,
                local_files_only=True,
            )
        except OSError:
            self._model = SentenceTransformer(
                model_name,
                revision=model_revision,
            )
        first_module = self._model[0]
        auto_model = getattr(first_module, "auto_model", None)
        model_config = getattr(auto_model, "config", None)
        resolved_revision = getattr(model_config, "_commit_hash", None)
        self.model_revision = resolved_revision or model_revision or "unresolved"

    def encode(self, texts: list[str], batch_size: int = 16) -> list[list[float]]:
        formatted_texts = [
            prepare_embedding_input(self.model_name, text) for text in texts
        ]
        embeddings = self._model.encode(
            formatted_texts,
            batch_size=batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return [list(map(float, embedding)) for embedding in embeddings]


def create_embedder(
    model_name: str,
    model_revision: Optional[str] = None,
) -> TextEmbedder:
    if model_name.strip().lower() == "hashing":
        return HashingEmbedder()
    return SentenceTransformerEmbedder(model_name, model_revision=model_revision)


def _enable_system_cert_store():
    try:
        import truststore
    except ImportError:
        return

    truststore.inject_into_ssl()


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0

    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))

    if left_norm == 0 or right_norm == 0:
        return 0.0

    return dot / (left_norm * right_norm)


def prepare_embedding_input(model_name: str, text: str) -> str:
    if model_name.startswith("intfloat/multilingual-e5"):
        return f"passage: {text}"
    return text


def embedding_input_fingerprint(model_name: str, text: str) -> str:
    exact_input = prepare_embedding_input(model_name, text)
    return hashlib.sha256(exact_input.encode("utf-8")).hexdigest()


def _hash_text(text: str, dimensions: int) -> list[float]:
    vector = [0.0] * dimensions
    for token in tokenize_for_similarity(text):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % dimensions
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vector[index] += sign
    return vector


def _normalize(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [value / norm for value in vector]
