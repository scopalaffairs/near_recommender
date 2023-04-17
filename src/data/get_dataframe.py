import demoji
import pandas as pd

from features.preprocessors import normalize_corpus


def get_dataframe(data, col_source, col_target, remove_links=None):
    df = pd.read_sql_query(data)
    df = df.drop_duplicates()
    df = df.dropna(subset=col_source).copy()
    df[col_target] = df[col_source].apply(
        lambda x: demoji.replace_with_desc(x, sep=" ")
    )
    df[col_target] = normalize_corpus(df[col_target].values, remove_links)

    return (df, list(df[col_target]))
