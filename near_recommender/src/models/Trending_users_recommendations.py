# Databricks notebook source
# MAGIC %md
# MAGIC ## Installations and imports

# COMMAND ----------

# MAGIC %pip install --upgrade scipy networkx cdlib

# COMMAND ----------

import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from cdlib import algorithms
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Graphs

# COMMAND ----------

# Load SQL tables as DF
follows_df = spark.sql('SELECT signer_id, follows, type FROM hive_metastore.sit.graph_follows').toPandas()
metrics_df = spark.sql('SELECT * FROM hive_metastore.sit.users_agg_metrics').toPandas().fillna(0)

# Define Graphs from follows/likes DF
G_follows = nx.from_pandas_edgelist(follows_df[follows_df['type']=='FOLLOW'], source='signer_id', target='follows')#, edge_attr=['type'])   #97% of actions are FOLLOWS

# Communities Detection
louvain_follows = algorithms.louvain(G_follows, weight='weight', resolution=1., randomize=False)
walktrap_follows = algorithms.walktrap(G_follows)


# COMMAND ----------

if louvain_follows.overlap == False:
    # Map Louvain follows communities to users
    lf_dict = {'signer_id': [], 'louvain_community': []}
    for i, community in enumerate(louvain_follows.communities):
        for user in community:
            lf_dict['signer_id'].append(user)
            lf_dict['louvain_community'].append(i)

    # Create a pandas dataframe from the data dictionary
    lf_df = pd.DataFrame(lf_dict)

if walktrap_follows.overlap == False:
    # Map Louvain follows communities to users
    wt_dict = {'signer_id': [], 'walktrap_community': []}
    for i, community in enumerate(walktrap_follows.communities):
        for user in community:
            wt_dict['signer_id'].append(user)
            wt_dict['walktrap_community'].append(i)

    # Create a pandas dataframe from the data dictionary
    wt_df = pd.DataFrame(wt_dict)

lcom_size_dict = dict(lf_df['louvain_community'].value_counts())
wtcom_size_dict = dict(wt_df['walktrap_community'].value_counts())

communities_df = pd.merge(lf_df, wt_df, on='signer_id')
communities_df['louvain_size'] = communities_df['louvain_community'].apply(lambda x: lcom_size_dict[x])
communities_df['walktrap_size'] = communities_df['walktrap_community'].apply(lambda x: wtcom_size_dict[x])

df = pd.merge(metrics_df, communities_df, on='signer_id', how='left')
df.shape

# COMMAND ----------

df['trending_metric'] = (df['engagement_weighted_30d'])/df['activity_weighted_30d']
trending_users_df = df[['signer_id', 
                        'followers',
                        'engagement_weighted_30d', 
                        'activity_weighted_30d', 
                        'louvain_community',
                        'trending_metric'
                        ]].sort_values('trending_metric', ascending=False).head(20)
trending_users_df

# COMMAND ----------

# List of Top10 communities by engagement
top10coms = list(df.groupby('louvain_community')['engagement_weighted_30d'].sum().sort_values(ascending=False).head(10).index) 

# List of first user by trending metric in each Top10 communities
top10coms_aux = df[['signer_id', 
                   'followers',
                   'engagement_weighted_30d', 
                   'activity_weighted_30d', 
                   'louvain_community',
                   'louvain_size',
                   'trending_metric'
                   ]][df['louvain_community'].isin(top10coms)]
top10coms_aux['rank'] = top10coms_aux.groupby('louvain_community')['trending_metric'].rank(method='first', ascending=False)
top10coms_df = top10coms_aux[top10coms_aux['rank']<3.0].sort_values('trending_metric', ascending=False)
top10coms_df

# COMMAND ----------

print(list(top10coms_df['signer_id']))

# COMMAND ----------

def n_trending_users(n):
    '''Return a random list of n users from the top 20 trending users'''
    return random.sample(list(trending_users_df['signer_id']), n)

def trending_users_outside_community (user, n):
    '''Return a random list of n users from the top 20 trending users in a different community than the input user'''
    user_community = df[df['signer_id']==user]['louvain_community'].values[0]
    aux_df = trending_users_df[trending_users_df['louvain_community']!=user_community]
    return random.sample(list(aux_df['signer_id']), n)


# COMMAND ----------

trending_users_outside_community('rojoser.near', 3)

# COMMAND ----------

n_trending_users(3)
