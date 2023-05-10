# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present.

import asyncio
import pickle
import threading
from typing import List, Tuple

import click
import pandas as pd
from sentence_transformers import SentenceTransformer, util

from near_recommender.src.data.get_dataframe import get_dataframe
from near_recommender.src.data.queries.query_get_post_text import query as posts_query
from near_recommender.src.features.top_sentences import return_similar_sentences
from near_recommender.src.features.utils import *

model = "all-mpnet-base-v2"
path = "near_recommender/"
filename = path + f"models/corpus_embeddings_{model}.pickle"
col_source = "post_text"
col_target = "clean_text"
data = "near_recommender/src/data/files/silver_posts.csv"


@click.command()
@click.option("--query", help="The query for the current post to find similarities on.")
def get_similar_post_users(
    query: str, data: str = data, top_k: int = 5
) -> List[Tuple[str, float, str, str]]:
    """
    Returns the top k most similar sentences in a corpus to a given query sentence.

    Args:
        query (str): The query sentence to find similar sentences for.
        top_k (int, optional): The number of top similar sentences to return. Defaults to 5.

    Returns:
        dict: A dictionary containing the top-k most similar sentences to the query.
    """
    embedder = SentenceTransformer(model)
    df, sentences = get_dataframe(data, col_source, col_target, remove_links=True)
    corpus_embeddings = load_pretrained_model(filename)
    top_n_sentences = return_similar_sentences(
        query=query,
        model_embedder=embedder,
        corpus_embeddings=corpus_embeddings,
        top_k=top_k,
        sentences=sentences,
        df=df
    )
    return {"similar_sentences": top_n_sentences}


def update_model():
    embedder = SentenceTransformer(model)
    _, sentences = get_dataframe(data, col_source, col_target, remove_links=True)
    corpus_embeddings = run_update_model(embedder, sentences)
    save_pretrained_model(filename, corpus_embeddings)
