import asyncio
import pickle
from typing import List

from sentence_transformers import SentenceTransformer, util


def load_pretrained_model(filename: str) -> None:
    with open(filename, 'rb') as f:
        return pickle.load(f)


async def save_pretrained_model(filename: str, cxt: str) -> None:
    with open(filename, 'wb') as f:
        pickle.dump(cxt, f)


async def run_update_model(
    model: SentenceTransformer, corpus: List
) -> SentenceTransformer:
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(None, update_model, model, corpus)
    corpus_embeddings = await future
    return corpus_embeddings.encode(corpus, convert_to_tensor=True)
