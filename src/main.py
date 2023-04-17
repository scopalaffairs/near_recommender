# -*- coding: utf-8 -*-
# (c) 2023 - present scopalaffairs

import asyncio

from data.queries.query_get_post_text import query as posts_query
from data.queries.query_get_post_text import query as tags_query
from models.similar_posts import get_similar_post_users
from models.similar_tags import get_similar_tags_users


async def task1():
    print("Starting task 1")
    await get_similar_post_users(data=posts_query, query="my query", top_k=5)
    print("Completed task 1")


async def task2():
    print("Starting task 2")
    idx=0
    await get_similar_tags_users(data=tags_query, idx=idx, top_k=10)
    print("Completed task 2")


async def main():
    await asyncio.gather(task1(), task2())


if __name__ == "__main__":
    asyncio.run(main())
