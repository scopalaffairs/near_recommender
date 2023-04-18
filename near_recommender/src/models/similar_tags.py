# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present.

import asyncio
from typing import Dict, List

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from near_recommender.src.features.preprocessors import (
    clean_profile_column,
    dissolve_nested_list,
)
from near_recommender.src.features.related_profile_tags import find_similar_users


async def get_similar_tags_users(
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
    signer = "signer_id"
    col_source = "profile"
    col_target = "tags"
    col_agg_tags = "aggregated_tags"

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

    return find_similar_users(profiles, col_agg_tags, idx, top_k)
