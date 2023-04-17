# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present.

import asyncio
import pickle
import threading
from typing import List, Tuple

import pandas as pd
from sentence_transformers import SentenceTransformer, util

from data.get_dataframe import get_dataframe
from features.top_sentences import return_similar_sentences
from features.utils import *


async def get_similar_post_users(
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
    lock = threading.Lock()
    model = "all-mpnet-base-v2"
    embedder = SentenceTransformer(model)
    path = "somewhere/file/storage"
    filename = path + f"models/corpus_embeddings_{model}.pickle"
    col_source = "post_text"
    col_target = "clean_text"
    df, sentences = get_dataframe(data, col_source, col_target, remove_links=True)

    t = threading.Thread(
        target=asyncio.run, args=(run_update_model(embedder, sentences),)
    )
    t.start()
    with lock:
        return return_similar_sentences(
            query=query,
            model_embedder=embedder,
            corpus_embeddings=load_pretrained_model(filename),
            top_k=top_k,
            sentences=sentences,
            df=df,
        )

    corpus_embeddings = await run_update_model(embedder, sentences) # just run on command
    await save_pretrained_model(filename, corpus_embeddings)
    t.join()
