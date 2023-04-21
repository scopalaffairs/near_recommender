query = """
SELECT
  signer_id,
  post:main:text AS post_text,
  block_timestamp
FROM
  hive_metastore.sit.near_social_txs_clean
WHERE
  post:main:text IS NOT NULL;
"""
