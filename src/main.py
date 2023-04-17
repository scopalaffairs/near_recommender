# -*- coding: utf-8 -*-
# (c) 2023 - present scopalaffairs

import asyncio

from data.queries.query_get_post_text import query as posts_query
from data.queries.query_get_post_text import query as tags_query
from models.similar_posts import get_similar_post_users
from models.similar_tags import get_similar_tags_users


async def main():
    try:
        similar_posts = asyncio.create_task(
            get_similar_post_users(data=posts_query, query="my query", top_k=5)
        )
        await similar_posts
        idx = 0
        similar_tags = asyncio.create_task(
            get_similar_tags_users(data=tags_query, idx=idx, top_k=10)
        )
        await similar_tags
    finally:
        loop.close()


asyncio.run(main())
