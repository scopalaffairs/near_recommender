# -*- coding: utf-8 -*-
# (c) 2023 - present scopalaffairs


from near_recommender.src.data.queries.query_get_post_text import query as posts_query
from near_recommender.src.data.queries.query_get_post_text import query as tags_query
from near_recommender.src.models.similar_posts import get_similar_post_users
from near_recommender.src.models.similar_tags import get_similar_tags_users





def task1():
    print("Starting task 1")
    # get_similar_post_users(data=posts_query, query="my query", top_k=5)
    print("Completed task 1")


def task2():
    print("Starting task 2")
    idx = 0
    # get_similar_tags_users(data=tags_query, idx=idx, top_k=10)
    print("Completed task 2")


def main(df):
    """
    Start with hive_metastore.sit.near_social_txs_clean table
    Join: graph_follows, graph_likes, aggregate User Metrics
    -> Trending Users Table
    Recommend Trending User to New User
    
    
    Todo:
        Recommendation system logic:
        If the user is new (< 1 week, < 3 days):
        → trending users
        If the user is not active:
        → trending users
        If the user is active:
        → friends-of-friends
        If the user has a tag:
        → tag similarity
        If the user has posted
        → post similarity
        If the user was inactive for some period:
        → trending users
        Bot detection (identifies known bots like littlelion, mr27, hypefairy):
        high % active days
        high post per day
        low follow_ratio
        …
        but difficult to filter out other account without further analysis of content, etc
        Definition of active users (proposals):
        following more than 5 users
        has any network activity (posted/liked/poked/added a tag)
        additional timeframe

    
    Implemented:
    
    Similar Tags can be called independently
    Similar Posts can be called independently
    Update Corpus for simlilar posts can be called indep
    
    
    Todo Daniel:
    Posts and Tags rewrite to work with hive_metastore.sit.near_social_txs_clean
    
    Todo ...
    
    
    
    """
    
    


if __name__ == "__main__":
    main(df)
