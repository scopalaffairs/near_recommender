import random

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from cdlib import algorithms


def get_trending_users():
    follows_df = spark.sql('SELECT signer_id, follows, type FROM hive_metastore.sit.graph_follows').toPandas()
    metrics_df = spark.sql('SELECT * FROM hive_metastore.sit.users_agg_metrics').toPandas().fillna(0)
    G_follows = nx.from_pandas_edgelist(follows_df[follows_df['type']=='FOLLOW'], source='signer_id', target='follows')
    louvain_follows = algorithms.louvain(G_follows, weight='weight', resolution=1., randomize=False)
    walktrap_follows = algorithms.walktrap(G_follows)

    if louvain_follows.overlap == False:
        lf_dict = {'signer_id': [], 'louvain_community': []}
        for i, community in enumerate(louvain_follows.communities):
            for user in community:
                lf_dict['signer_id'].append(user)
                lf_dict['louvain_community'].append(i)

        lf_df = pd.DataFrame(lf_dict)

    if walktrap_follows.overlap == False:
        wt_dict = {'signer_id': [], 'walktrap_community': []}
        for i, community in enumerate(walktrap_follows.communities):
            for user in community:
                wt_dict['signer_id'].append(user)
                wt_dict['walktrap_community'].append(i)

        wt_df = pd.DataFrame(wt_dict)

    lcom_size_dict = dict(lf_df['louvain_community'].value_counts())
    wtcom_size_dict = dict(wt_df['walktrap_community'].value_counts())

    communities_df = pd.merge(lf_df, wt_df, on='signer_id')
    communities_df['louvain_size'] = communities_df['louvain_community'].apply(lambda x: lcom_size_dict[x])
    communities_df['walktrap_size'] = communities_df['walktrap_community'].apply(lambda x: wtcom_size_dict[x])

    df = pd.merge(metrics_df, communities_df, on='signer_id', how='left')

    df['trending_metric'] = (df['engagement_weighted_30d'])/df['activity_weighted_30d']
    trending_users_df = df[['signer_id',
                            'followers',
                            'trending_metric',
                            'louvain_community'
                            ]].sort_values('trending_metric', ascending=False).head(20).reset_index(drop=True)

    trending_users_df['followers'] = trending_users_df['followers'].apply(int)
    trending_users_df['louvain_community'] = trending_users_df['louvain_community'].apply(int)
    trending_users_df.index = trending_users_df.index + 1
    trending_users_df.rename(columns={'signer_id': 'user_name', 'louvain_community': 'com_ID'}, inplace=True)

    id_to_name = {0.0: 'Aurora Network',
                  1.0: 'Core Devs',
                  3.0: 'Near Ukraine',
                  5.0: 'Near Foundation',
                  8.0: 'NearXArt Dao',
                  10.0: 'Learn Near Club',
                  16.0: 'Near Nigeria'}

    trending_users_df['com_name'] = trending_users_df['com_ID'].map(id_to_name)

    return trending_users_df[['user_name', 'com_ID']].to_json