# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present.


from typing import Dict, List

from pyspark.sql import SparkSession
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from near_recommender.src.data import get_dataframe
from near_recommender.src.data.queries.query_get_profile_tags import query as tags_query
from near_recommender.src.features.preprocessors import (
    clean_profile_column,
    dissolve_nested_list,
)
from near_recommender.src.features.related_profile_tags import find_similar_users

signer = "signer_id"
col_source = "profile"
col_target = "tags"
col_agg_tags = "aggregated_tags"

spark = SparkSession.builder.getOrCreate()
result = spark.sql(tags_query)
data = result.toPandas()


def get_similar_tags_users(
    idx: int, data: str, top_k: int = 5
) -> List[Dict[str, List[str]]]:
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
    tags[col_source] = clean_profile_column(tags[col_source])
    tags.dropna(subset=col_source).copy()
    tags[col_target] = tags[col_source].apply(
        lambda x: [
            normalize_document(k) if isinstance(k, str) else k for k, _ in x.items()
        ]
    )
    profiles = (
        tags.groupby(signer)[col_target].apply(list).reset_index(name=col_agg_tags)
    )
    profiles[col_agg_tags] = profiles[col_agg_tags].apply(dissolve_nested_list)

    similar_users = find_similar_users(profiles, col_agg_tags, idx, top_k)
    response_dict = json.dumps(
        {
            "similar_users": [
                {"user_id": user_id, "similarity_score": similarity_score}
                for user_id, similarity_score in similar_users
            ]
        }
    )
    return response_dict
