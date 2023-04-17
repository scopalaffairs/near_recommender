import re

import contractions
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
    doc = re.sub(r'[^a-zA-Z\s]', ' ', doc, re.I | re.A)
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


def clean_profile_column(series):
    def remove_null_values(d):
        return {k: v for k, v in d.items() if v is not None}

    cleaned_values = series.apply(
        lambda x: remove_null_values(json.loads(x)) if pd.notnull(x) else None
    )
    cleaned_values = cleaned_values[cleaned_values.apply(lambda x: bool(x))]

    return cleaned_values


def dissolve_nested_list(lst):
    return [element for sublist in lst for element in sublist]
