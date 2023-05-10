# -*- coding: utf-8 -*-
# (c) scopalaffairs 2023 - present


import json
from datetime import datetime, timedelta
from typing import Dict, List

import boto3

from near_recommender.src.models.friends_friends import get_friends_of_friends
from near_recommender.src.models.similar_posts import get_similar_post_users
from near_recommender.src.models.similar_tags import get_similar_tags_users
from near_recommender.src.models.trending_users import get_trending_users


def main(users: List[Dict[str, any]]) -> None:
    s3 = boto3.client('s3')
    bucket_name = 'near-public-lakehouse'

    today = datetime.today().date()
    trending_days = 7
    inactive_days = 30

    for user in users:
        created_date = datetime.strptime(user['created_at'], '%Y-%m-%d').date()
        if (today - created_date).days < 7:
            if (today - created_date).days < 3:
                recommendations = get_trending_users(top_k=5)
            else:
                recommendations = get_trending_users(top_k=3)
        else:
            last_active_date = datetime.strptime(
                user['last_active_at'], '%Y-%m-%d'
            ).date()
            if (today - last_active_date).days > inactive_days:
                recommendations = get_trending_users(top_k=5)
            else:
                if user.get('tags'):
                    data = f"SELECT * FROM users WHERE user_id='{user['id']}'"
                    recommendations = get_similar_tags_users(user['id'], data, top_k=5)
                elif user.get('posts'):
                    query = user['posts']
                    recommendations = get_similar_post_users(
                        query, model_embedder, corpus_embeddings, top_k, sentences, df
                    )
                else:
                    recommendations = get_friends_of_friends(user['id'], top_k=5)

        filename = f"{user['id']}_recommendations.json"
        s3.put_object(
            Bucket=bucket_name, Key=filename, Body=json.dumps(recommendations)
        )


if __name__ == "__main__":
    main(df)
