# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present.

import asyncio
import pickle
import threading
from typing import List, Tuple

import pandas as pd
from sentence_transformers import SentenceTransformer, util

from near_recommender.data.get_dataframe import get_dataframe
from near_recommender.data.queries.query_get_post_text import \
    query as posts_query
from near_recommender.features.top_sentences import return_similar_sentences
from near_recommender.features.utils import *

model = "all-mpnet-base-v2"
path = "./"
filename = path + f"models/corpus_embeddings_{model}.pickle"
col_source = "post_text"
col_target = "clean_text"
data = "near_recommender/data/files/silver_posts.csv"

def get_similar_post_users(
    data: str, query: str, top_k: int = 5
) -> List[Tuple[str, float, str, str]]:
    """
    Returns the top k most similar sentences in a corpus to a given query sentence.

    Args:
        query (str): The query sentence to find similar sentences for.
        top_k (int, optional): The number of top similar sentences to return. Defaults to 5.

    Returns:
        List[Tuple[str, float, str, str]]: A list of tuples, where each tuple contains:
            1. The similar sentence to the query.
            2. The similarity score between the query and the similar sentence.
            3. The original post text of the similar sentence.
            4. The cleaned post text of the similar sentence.
    """
    df, sentences = get_dataframe(data, col_source, col_target, remove_links=True)

    return return_similar_sentences(
        query=query,
        model_embedder=embedder,
        corpus_embeddings=load_pretrained_model(filename),
        top_k=top_k,
        sentences=sentences,
        df=df,
    )


def update_model():
    embedder = SentenceTransformer(model)
    _, sentences = get_dataframe(data, col_source, col_target, remove_links=True)
    corpus_embeddings = run_update_model(embedder, sentences)
    save_pretrained_model(filename, corpus_embeddings)
