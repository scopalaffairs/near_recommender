# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present.


import json
from typing import Dict, List

from pyspark.sql import SparkSession
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from near_recommender.src.data.get_dataframe import get_dataframe
from near_recommender.src.data.queries.query_get_profile_tags import query as tags_query
from near_recommender.src.features.related_profile_tags import find_similar_users

signer = "signer_id"
col_source = "profile"
col_target = "tags"
col_agg_tags = "aggregated_tags"

spark = SparkSession.builder.getOrCreate()
result = spark.sql(tags_query)
data = result.toPandas()


def get_similar_tags_users(idx: int, top_k: int = 5) -> List[Dict[str, List[str]]]:
    """Returns the top-k users with similar tags as the specified user.

    Args:
        idx (int): The index of the user for whom similar users are to be found.
        data (str): The query to fetch data from the database, SQL from table.
        top_k (int, optional): The number of similar users to be returned. Defaults to 5.

    Returns:
        dict: A dictionary containing the top-k similar users and their similarity scores.

    Raises:
        ValueError: If the input dataframe is empty or contains NaN values.
        TypeError: If the input top_k value is not an integer.

    """

    tags, _ = get_dataframe(data, col_source, col_target)
    tags.dropna(subset=col_source).copy()
    profiles = (
        tags.groupby(signer)[col_target].apply(list).reset_index(name=col_agg_tags)
    )
    profiles[col_agg_tags] = profiles[col_agg_tags].apply(
        lambda x: [word for sublist in x for word in sublist.split()]
    )
    similar_users = find_similar_users(profiles, col_agg_tags, idx, top_k)
    response_dict = json.dumps(
        {
            "similar_tags": [
                {
                    "signer_id": item['similar_profile']['signer_id'],
                    "similarity_score": item['score'],
                }
                for item in similar_users
            ]
        }
    )
    return response_dict
