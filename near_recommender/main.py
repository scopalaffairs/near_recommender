# -*- coding: utf-8 -*-
# (c) 2023 - present scopalaffairs


from near_recommender.data.queries.query_get_post_text import query as posts_query
from near_recommender.data.queries.query_get_post_text import query as tags_query
from near_recommender.models.similar_posts import get_similar_post_users
from near_recommender.models.similar_tags import get_similar_tags_users


def task1():
    print("Starting task 1")
    # get_similar_post_users(data=posts_query, query="my query", top_k=5)
    print("Completed task 1")


def task2():
    print("Starting task 2")
    idx = 0
    # get_similar_tags_users(data=tags_query, idx=idx, top_k=10)
    print("Completed task 2")


def main():
    print("What")


if __name__ == "__main__":
    main()
