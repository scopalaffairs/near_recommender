# Databricks notebook source
# MAGIC %pip install mlflow
# MAGIC %pip install xgboost

# COMMAND ----------

import mlflow
import mlflow.sklearn

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
 
from numpy import savetxt
 
from sklearn.model_selection import train_test_split
 
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.metrics import confusion_matrix

import xgboost as xgb

# COMMAND ----------

SparkDF=spark.read.format("csv").option("delimiter", ",").option("header","true").load("/FileStore/tables/active_user_follow_matrix.csv")
SparkDF=SparkDF.drop('ConnectionID')
SparkDF=SparkDF.withColumnRenamed("AccountID","User").withColumnRenamed("FollowerID","Follower")

#SparkDF.printSchema()
#SparkDF.show(truncate=True)

PandasDF=SparkDF.toPandas()
PandasDF["followed"] =PandasDF["followed"].replace({"True": 1, "False": 0})
PandasDF["followed"] =PandasDF["followed"].astype(float)
PandasDF["User"] =PandasDF["User"].astype(float)
PandasDF["Follower"] =PandasDF["Follower"].astype(float)

#PandasDF.head()

features=PandasDF.drop('followed',axis=1)
labels=PandasDF.drop('User',axis=1).drop('Follower',axis=1)

train_features,test_features,train_labels,test_labels = train_test_split(features,labels, test_size=0.2)

# COMMAND ----------

dtrain = xgb.DMatrix(train_features[["User","Follower"]], label=train_labels["followed"])

param = {'max_depth': 2, 'eta': 1, 'silent': 2, 'objective': 'multi:softmax'}
param['nthread'] = 4
param['eval_metric'] = 'auc'
param['num_class'] = 6
#param ['enable_categorical']=True

# COMMAND ----------

num_round = 10
bst = xgb.train(param, dtrain, num_round)

# COMMAND ----------

dtest = xgb.DMatrix(test_features[["User","Follower"]])
ypred = bst.predict(dtest)

# COMMAND ----------

#np.set_printoptions(threshold=1000)
arr=np.where(ypred==1)
print(arr)
#print(ypred)

# COMMAND ----------

from sklearn.metrics import precision_score

pre_score = precision_score(test_labels["followed"],ypred, average='micro')

print("xgb_pre_score:",pre_score)