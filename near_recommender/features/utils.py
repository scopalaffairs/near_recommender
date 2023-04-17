import asyncio
import pickle
from typing import List

from sentence_transformers import SentenceTransformer, util


def load_pretrained_model(filename: str) -> None:
    with open(filename, 'rb') as f:
        return pickle.load(f)


def save_pretrained_model(filename: str, cxt: str) -> None:
    with open(filename, 'wb') as f:
        pickle.dump(cxt, f)


def run_update_model(
    model: SentenceTransformer, corpus: List
) -> SentenceTransformer:
    return model.encode(corpus, convert_to_tensor=True)
