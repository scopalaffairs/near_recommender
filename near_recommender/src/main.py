# -*- coding: utf-8 -*-
# (c) 2023 - present scopalaffairs


from near_recommender.src.data.queries.query_get_post_text import query as posts_query
from near_recommender.src.data.queries.query_get_post_text import query as tags_query
from near_recommender.src.models.similar_posts import get_similar_post_users
from near_recommender.src.models.similar_tags import get_similar_tags_users


NEW_ACCOUNT_AGE = 7 # in days, TODO: refine
TIME_DEF = 60 # in days, TODO: refine


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
        
    Todo Agustin:
        Reorganize User metrics table to add all users
        Separate table for Trending Users (interface for main functions)
    """
    
    # new users and inactive
    if address_age < NEW_ACCOUNT_AGE or recency > TIME_DEF:
        generic_trending_users()
    
    # older users and active ones
    else:
        if user_has_tag:
            output = similar_tags()
            print(f"Based on your tags, this similar are recommended: {output}")
        if user_has_posted:
            similar_posts()
        else:
            trending_users_outside_community()

if __name__ == "__main__":
    main(df)
