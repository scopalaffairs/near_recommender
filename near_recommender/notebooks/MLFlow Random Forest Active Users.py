# Databricks notebook source
# MAGIC %pip install mlflow

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

#PandasDF.head()

features=PandasDF.drop('followed',axis=1)
labels=PandasDF.drop('User',axis=1).drop('Follower',axis=1)

train_features,test_features,train_labels,test_labels = train_test_split(features,labels, test_size=0.2)


# COMMAND ----------

#train_features.shape
#train_labels.shape
#test_features.shape
#test_labels.shape
#test_features.head()
#test_labels.head()
#test_labels.describe()

# COMMAND ----------

# Enable autolog()
# mlflow.sklearn.autolog() requires mlflow 1.11.0 or above.
mlflow.sklearn.autolog()
 
# With autolog() enabled, all model parameters, a model score, and the fitted model are automatically logged.  
with mlflow.start_run(run_name='confusion_matrix'):
    #mlflow.log_confusion_matrix(conf_mat, labels=['0', '1'])
  
# Set the model parameters. 
    n_estimators = 100
    max_depth = 6
    max_features = 2
  
    # Create and train model.
    rf = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, max_features = max_features)
    rf.fit(train_features, train_labels)
  
    # Use the model to make predictions on the test dataset.
    predictions = rf.predict(test_features)

#conf_matrix = confusion_matrix(test_labels,predictions)
#true_positive = conf_matrix[0][0]
#true_negative = conf_matrix[1][1]
#false_positive = conf_matrix[0][1]
#false_negative = conf_matrix[1][0]

#mlflow.log_metric("true_positive", true_positive)
#mlflow.log_metric("true_negative", true_negative)
#mlflow.log_metric("false_positive", false_positive)
#mlflow.log_metric("false_negative", false_negative)

#And then log_artifact(<Test 1>, "confusion_matrix")

    #conf_mat = confusion_matrix(test_labels,predictions)

#with mlflow.start_run(run_name='confusion_matrix'):
    #mlflow.log_confusion_matrix(conf_mat, labels=['0', '1'])

# COMMAND ----------

#print(predictions)
#predictions.shape
test_labels.shape
#test_labels.head()
#print(test_labels)

# COMMAND ----------

# get confusion matrix values
conf_matrix = confusion_matrix(test_labels,predictions)
true_positive = conf_matrix[0][0]
true_negative = conf_matrix[1][1]
false_positive = conf_matrix[0][1]
false_negative = conf_matrix[1][0]

mlflow.log_metric("true_positive", true_positive)
mlflow.log_metric("true_negative", true_negative)
mlflow.log_metric("false_positive", false_positive)
mlflow.log_metric("false_negative", false_negative)

#And then log_artifact(<Test 1>, "confusion_matrix")

# COMMAND ----------

conf_mat = confusion_matrix(test_labels,predictions)

with mlflow.start_run(run_name='confusion_matrix'):
    mlflow.log_confusion_matrix(conf_mat, labels=['0', '1'])