import os

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import confusion_matrix, precision_score
from sklearn.model_selection import train_test_split


def get_friends_of_friends(spark_df_path):
    spark_df = (
        spark.read.format("csv")
        .option("delimiter", ",")
        .option("header", "true")
        .load(spark_df_path)
    )

    spark_df = spark_df.drop('ConnectionID')
    spark_df = spark_df.withColumnRenamed("AccountID", "User").withColumnRenamed(
        "FollowerID", "Follower"
    )

    pandas_df = spark_df.toPandas()
    pandas_df["followed"] = pandas_df["followed"].replace({"True": 1, "False": 0})
    pandas_df["followed"] = pandas_df["followed"].astype(float)

    pandas_df["User"] = pandas_df["User"].astype(float)
    pandas_df["Follower"] = pandas_df["Follower"].astype(float)

    features = pandas_df.drop('followed', axis=1)
    labels = pandas_df.drop('User', axis=1).drop('Follower', axis=1)
    train_features, test_features, train_labels, test_labels = train_test_split(
        features, labels, test_size=0.2
    )

    dtrain = xgb.DMatrix(
        train_features[["User", "Follower"]], label=train_labels["followed"]
    )
    param = {'max_depth': 2, 'eta': 1, 'silent': 2, 'objective': 'multi:softmax'}
    param['nthread'] = 4
    param['eval_metric'] = 'auc'
    param['num_class'] = 6
    num_round = 10
    bst = xgb.train(param, dtrain, num_round)

    dtest = xgb.DMatrix(test_features[["User", "Follower"]])
    ypred = bst.predict(dtest)

    pre_score = precision_score(test_labels["followed"], ypred, average='micro')
    print("xgb_pre_score:", pre_score)

    arr = np.where(ypred == 1)
    return {"predicted_users": arr[0].tolist()}
