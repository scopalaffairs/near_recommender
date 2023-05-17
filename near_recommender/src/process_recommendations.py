import json

# import boto3
# from pyspark.sql import SparkSession

from near_recommender import get_recommendations_per_user
from near_recommender.src.data.queries.query_all_users import users as all_users


def process_recommendations(verbose=False):
    """
    This gets all recommendations for a given user from all_users.
    Writes a JSON object for each user to an S3 bucket.
    """

    # access_key = dbutils.secrets.get(scope="aws", key="access-key")
    # secret_key = dbutils.secrets.get(scope="aws", key="secret-access-key")

    # s3 = boto3.client('s3')
    # bucket_name = 'near-public-lakehouse'
    # region_name = 'eu-central-1'

    # spark = SparkSession.builder.getOrCreate()
    # result = spark.sql(all_users)
    # users = result.toPandas()

    # for idx, user in users.iterrows():
        # try:
            # rec = get_recommendations_per_user(idx=idx, user=user)
            # recommendations = json.dumps(rec)
            # file_name = f"silver/near-social/{user['signer_id']}.json"

            # s3.put_object(
                # aws_access_key_id=access_key,
                # aws_secret_access_key=secret_key,
                # region_name=region_name,
                # Bucket=bucket_name,
                # Key=file_name,
                # Body=recommendations.encode('utf-8'),
                # ContentType='application/json',
            # )

            # if verbose:
                # print(recommendations)

        # except Exception as e:
            # print(f'Error processing user {idx}, {user["signer_id"]}: {e}')


process_recommendations(verbose=False)
