import pickle
from typing import List

import pandas as pd
from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, util

from near_recommender.src.data.queries.query_get_post_text import query as posts_query


def load_pretrained_model(filename: str):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def save_pretrained_model(filename: str, cxt: str) -> None:
    with open(filename, 'wb') as f:
        pickle.dump(cxt, f)


def run_update_model(model: SentenceTransformer, corpus: List) -> SentenceTransformer:
    return model.encode(corpus, convert_to_tensor=True)


def filter_last_post(signer_id):
    spark = SparkSession.builder.getOrCreate()
    result = spark.sql(posts_query)
    df = result.toPandas()
    df = df.sort_values(['signer_id', 'block_timestamp'], ascending=[True, False])
    df = df.drop_duplicates('signer_id')
    filtered_df = df[df['signer_id'] == signer_id]

    return filtered_df
