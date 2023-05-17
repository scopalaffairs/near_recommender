# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present


import json
from typing import Dict, List

import pandas as pd

from near_recommender.src.features.utils import filter_last_post
from near_recommender.src.models.friends_friends import get_friends_of_friends
from near_recommender.src.models.similar_posts import get_similar_post_users
from near_recommender.src.models.similar_tags import get_similar_tags_users
from near_recommender.src.models.trending_users import get_trending_users


def get_recommendations_per_user(
    idx: int, user: pd.Series, address_age: int = 7, top_k: int = 5
) -> List[Dict]:
    """
    Gets all applicable recommendations for a given user.

    Recommendation system logic:
        - If the user is new (< 1 week, < 3 days): appends trending users
        - If the user is not active: appends trending users
        - If the user is active: appends friends-of-friends
        - If the user has a tag: appends tag similarity
        - If the user has posted: appends post similarity
        - If the user was inactive for some period: appends trending users

    Returns a list of dictionaries with signer_id and recommended users.
    """
    recommendations = []
    if user["address_age"] < address_age:
        print(f"new user {address_age}: {user['signer_id']}")
        trending_users = get_trending_users()
        recommendations.append(trending_users)
    elif user["active_last_month"]:
        print(f"inactive user: {user['signer_id']}")
        trending_users = get_trending_users()
        recommendations.append(trending_users)
    if user["user_has_tags"]:
        print(f"user has tags: {user['signer_id']}")
        similar_tags = get_similar_tags_users(idx, top_k=top_k)
        recommendations.append(similar_tags)
    if user["user_has_posted"]:
        print(f"user posted: {user['signer_id']}")
        post = filter_last_post(user["signer_id"])
        if len(post) > 0:
            string_value = post.values[0]
            similar_post = get_similar_post_users(string_value, top_k=top_k)
            recommendations.append(similar_post)
    else:
        print("friends of friends")
        # recommendations = get_friends_of_friends(idx)
    print(f"{recommendations=}")

    return recommendations
