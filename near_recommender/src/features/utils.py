import glob
import pickle
from typing import List

import pandas as pd
from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, util

from near_recommender.src.data.queries.query_get_post_text import query as posts_query


def load_pretrained_model(base_filename: str):
    filenames = glob.glob(f"{base_filename}_*.pickle")
    if len(filenames) == 0:
        print(
            "No pre-trained model available\n",
            "Run update_corpus to generate pooled word embeddings.",
        )
        return None
    filenames.sort(key=lambda x: int(x.split("_")[-1].split(".")[0]), reverse=True)
    latest_model_file = filenames[0]
    with open(latest_model_file, "rb") as f:
        return pickle.load(f)


def save_pretrained_model(base_filename: str, cxt: str) -> None:
    version = 0
    while os.path.exists(f"{base_filename}_{version}.pickle"):
        version += 1
    filename = f"{base_filename}_{version}.pickle"
    with open(filename, "wb") as f:
        pickle.dump(cxt, f)


def run_update_model(model: SentenceTransformer, corpus: List) -> SentenceTransformer:
    return model.encode(corpus, convert_to_tensor=True)


def filter_last_post(signer_id):
    spark = SparkSession.builder.getOrCreate()
    result = spark.sql(posts_query)
    df = result.toPandas()
    df = df.sort_values(["signer_id", "block_timestamp"], ascending=[True, False])
    df = df.drop_duplicates("signer_id")
    filtered_df = df[df["signer_id"] == signer_id]

    return filtered_df


def get_index_from_signer_id(signer_id, dataset):
    idx = dataset[dataset["signer_id"] == signer_id].index
    if len(idx) > 0:
        return idx[0]
    raise ValueError(f"Signer ID {signer_id} not found in the dataset.")
