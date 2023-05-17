from typing import Dict, List, Tuple

import pandas as pd
import torch
from sentence_transformers import SentenceTransformer, util


def return_similar_sentences(
    query: str,
    model_embedder: SentenceTransformer,
    corpus_embeddings: torch.Tensor,
    top_k: int,
    df: pd.DataFrame,
    sentences: List[str],
) -> List[Tuple[str, float, str, pd.Series]]:
    """
    Returns the top k most similar sentences in the corpus to the given query, along with their similarity scores,
    associated DataFrame row, and username.

    :param query: str, the query to compare with the corpus sentences.
    :param model_embedder: SentenceTransformer, a sentence embedding model to encode the query and corpus sentences.
    :param corpus_embeddings: torch.Tensor, a list of lists of sentence embeddings for the corpus sentences.
    :param top_k: int, the number of top similar sentences to return.
    :param df: pd.DataFrame, a pandas DataFrame containing the corresponding row for each sentence in the corpus.
    :param sentences: List[str], a list of the original sentences in the corpus.
    :return: List[Tuple[str, float, str, pd.Series]], a list of tuples containing the top similar sentences, their
    similarity scores, the associated username and the corresponding DataFrame row.
    """
    query_embedding = model_embedder.encode(query, convert_to_tensor=True)
    cos_scores = util.pytorch_cos_sim(query_embedding, corpus_embeddings)[0]
    cos_scores = cos_scores.cpu()
    top_results = torch.topk(cos_scores, k=top_k)

    top_n_sentences = []
    for score, idx in zip(top_results[0], top_results[1]):
        sentence = sentences[int(idx)]
        similarity_score = float(score)
        username = df.iloc[int(idx)]['signer_id']
        post_id = df.iloc[int(idx)].name
        top_n_sentences.append((sentence, similarity_score, username, post_id))

    return top_n_sentences
