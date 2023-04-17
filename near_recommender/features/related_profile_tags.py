from typing import Dict, List

import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def find_similar_users(
    profiles: pd.DataFrame, col_agg_tags: str, idx: int, top_k: int
) -> List[Dict[str, any]]:
    """
    Given a DataFrame of user profiles, a column name for aggregated tags, an index for the target user,
    and a number k, this function returns a dictionary with a list of k user profiles similar to the target user,
    along with their similarity scores.

    :param profiles: a DataFrame of user profiles.
    :param col_agg_tags: a string representing the column name for the aggregated tags.
    :param idx: an integer representing the index of the target user.
    :param top_k: an integer representing the number of similar users to return.
    :return: List[Dict[str, any]], a list of k user profiles similar to the target user, along with their similarity scores.
    Each user profile is represented as a dictionary with two keys: "score" and "similar_profile". The value
    associated with the "score" key is a float representing the cosine similarity score between the target user
    and the similar user.
    :raises: ValueError: If the value of idx is invalid or if no similar users are found for the given input.
    """
    vectorizer = CountVectorizer(analyzer=lambda x: x)
    tag_matrix = vectorizer.fit_transform(profiles[col_agg_tags])
    cosine_sim = cosine_similarity(tag_matrix)

    if idx >= cosine_sim.shape[0]:
        raise ValueError(f"Invalid value for idx: {idx}")

    similar_users = cosine_sim[idx].argsort()[::-1][: top_k + 1]

    if not similar_users.any():
        raise ValueError("No similar users found for the given input.")

    recommended_profiles = []
    for i in similar_users:
        if i == idx:
            continue
        else:
            score = cosine_sim[idx][i]
            recommended_profiles.append(
                {"score": score, "similar_profile": profiles.iloc[i].to_dict()}
            )

    return recommended_profiles[:top_k]
