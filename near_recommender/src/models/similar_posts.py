# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present.


import os
from functools import lru_cache
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, util

from near_recommender.src.data.get_dataframe import get_dataframe
from near_recommender.src.data.queries.query_get_post_text import query as posts_query
from near_recommender.src.features.top_sentences import return_similar_sentences
from near_recommender.src.features.utils import *

model = "all-mpnet-base-v2"
path = "/dbfs/FileStore/models/"
filename = path + f"corpus_embeddings_{model}"
col_source = "post_text"
col_target = "clean_text"

spark = SparkSession.builder.getOrCreate()


@lru_cache(maxsize=1)
def load_corpus_embeddings(
    filename: str,
) -> Tuple[object, list[str], object, SentenceTransformer]:
    """
    Loads the corpus embeddings from a given filename using a SentenceTransformer model.

    :param filename: The filename of the pretrained model to load the corpus embeddings from.
    :type filename: str
    :return: A tuple containing the loaded corpus embeddings, the list of sentences, the DataFrame, and the SentenceTransformer model.
    :rtype: Tuple[object, list[str], object, SentenceTransformer]
    """
    result = spark.sql(posts_query)
    data = result.toPandas()

    embedder = SentenceTransformer(model)
    df, sentences = get_dataframe(data, col_source, col_target, remove_links=True)
    corpus_embeddings = load_pretrained_model(filename)
    return corpus_embeddings, sentences, df, embedder


def get_similar_post_users(
    query: str, top_k: int = 5
) -> Dict[str, List[Tuple[str, float, str, str]]]:
    """
    Returns the top k most similar sentences in a corpus to a given query sentence.

    :param query: The query sentence to find similar sentences for.
    :type query: str
    :param top_k: The number of top similar sentences to return. Defaults to 5.
    :type top_k: int, optional
    :return: A dictionary containing the top-k most similar sentences to the query.
    :rtype: dict
    """
    corpus_embeddings, sentences, df, embedder = load_corpus_embeddings(filename)
    top_n_sentences = return_similar_sentences(
        query=query,
        model_embedder=embedder,
        corpus_embeddings=corpus_embeddings,
        top_k=top_k,
        sentences=sentences,
        df=df,
    )
    return {"similar_posts": top_n_sentences}


def update_corpus() -> None:
    """
    Updates a large language NLP sentence transformer model with new data.
    The model is saved to the location specified in the `path` variable.

    :return: None
    :rtype: None
    """
    embedder = SentenceTransformer(model)
    _, sentences = get_dataframe(data, col_source, col_target, remove_links=True)
    corpus_embeddings = run_update_model(embedder, sentences)
    save_pretrained_model(filename, corpus_embeddings)
