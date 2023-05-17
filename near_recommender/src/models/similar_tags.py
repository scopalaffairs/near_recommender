# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present.


import json
import os
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


def get_similar_tags_users(
    user: str, top_k: int = 5
) -> Dict[str, List[Dict[str, str]]]:
    """
    Returns the top-k users with similar tags as the specified user.

    :param user: The name of the user for whom similar users are to be found.
    :type user: str
    :param top_k: The number of similar users to be returned. Defaults to 5.
    :type top_k: int, optional
    :return: A dictionary containing the top-k similar users and their similarity score.
    :rtype: Dict[str, List[Dict[str, str]]]
    :raises ValueError: If the input dataframe is empty or contains NaN values.
    :raises TypeError: If the input top_k value is not an integer.
    """

    result = spark.sql(tags_query)
    data = result.toPandas()

    tags, _ = get_dataframe(data, col_source, col_target)
    tags.dropna(subset=col_source).copy()
    profiles = (
        tags.groupby(signer)[col_target].apply(list).reset_index(name=col_agg_tags)
    )
    profiles[col_agg_tags] = profiles[col_agg_tags].apply(
        lambda x: [word for sublist in x for word in sublist.split()]
    )
    similar_users = find_similar_users(profiles, col_agg_tags, user, top_k)
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
