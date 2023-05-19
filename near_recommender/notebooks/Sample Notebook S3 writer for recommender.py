# Databricks notebook source
import json
import boto3

# COMMAND ----------

# set up AWS credentials
access_key = dbutils.secrets.get(scope="aws", key="access-key")
secret_key = dbutils.secrets.get(scope="aws", key="secret-access-key")
region_name = 'eu-central-1'

# create an S3 client with credentials
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region_name
)

# COMMAND ----------

# TODO: Add here the code that will produce the json object from the recommender. This is just an example, please feel free to structure the json
json_obj = json.dumps("""
    {
        "recommendations": [
            "recommendation": {
                "user": "a",
                "rank": 1
            }, 
            "recommendation": {
                "user": "b",
                "rank": 2
            }, 
        ]
    }
""")

# COMMAND ----------

file_name = f'silver/near-social/some-user.json'

try:
    s3.put_object(
        Bucket='near-public-lakehouse', 
        Key=file_name,
        Body=json_obj.encode('utf-8'),
        ContentType='application/json'
    )
except Exception as e:
    print('s3.put_object error: ', e)

# COMMAND ----------


