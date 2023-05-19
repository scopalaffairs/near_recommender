import json
import re

import contractions
import nltk
import pandas as pd
import tqdm


def normalize_document(doc, remove_links=None):
    if remove_links:
        doc = re.sub(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
            ' ',
            doc,
        )
    doc = doc.translate(doc.maketrans("\n\t\r", "   "))
    doc = doc.lower()
    doc = contractions.fix(doc)
    doc = re.sub(r'[^a-zA-Z0-9\s]', ' ', doc, re.I | re.A)
    doc = re.sub(' +', ' ', doc)
    doc = re.sub('\{\}\"', '', doc)
    doc = doc.strip()

    return doc


def normalize_corpus(docs, remove_links=None):
    norm_docs = []
    for doc in tqdm.tqdm(docs):
        norm_doc = normalize_document(doc, remove_links)
        norm_docs.append(norm_doc)

    return norm_docs
