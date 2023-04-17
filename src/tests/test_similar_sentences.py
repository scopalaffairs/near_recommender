from typing import Dict, List

import pandas as pd
import pytest
from sentence_transformers import SentenceTransformer, util

from features.top_sentences import return_similar_sentences

model = "paraphrase-distilroberta-base-v1"


@pytest.fixture
def model_embedder():
    return SentenceTransformer(model)


@pytest.fixture
def corpus_embeddings(model_embedder):
    sentences = [
        "This is an example sentence",
        "Here's another example sentence",
        "This is a third sentence",
        "Yet another sample sentence",
    ]
    return model_embedder.encode(sentences)


@pytest.fixture
def df():
    return pd.DataFrame({'signer_id': ['user1', 'user2', 'user3', 'user4', 'user5']})


@pytest.fixture
def sentences():
    return [
        "This is an example sentence",
        "Here's another example sentence",
        "This is a third sentence",
        "Yet another sample sentence",
    ]


def test_return_similar_sentences(model_embedder, corpus_embeddings, df, sentences):
    query = "This is a sample query"
    top_k = 2

    results = return_similar_sentences(
        query=query,
        model_embedder=model_embedder,
        corpus_embeddings=corpus_embeddings,
        top_k=top_k,
        df=df,
        sentences=sentences
    )

    assert isinstance(results, list)
    assert len(results) == top_k

    for result in results:
        assert isinstance(result, tuple)
        assert len(result) == 4
        assert isinstance(result[0], str)
        assert isinstance(result[1], float)
        assert isinstance(result[2], str)

    scores = [result[1] for result in results]
    assert scores == sorted(scores, reverse=True)

    expected_sentences = ["This is an example sentence", "Yet another sample sentence"]
    for result, expected_sentence in zip(results, expected_sentences):
        assert result[0] == expected_sentence
