query = """
SELECT
  signer_id,
  post:main:text AS post_text,
  block_timestamp
FROM
  hive_metastore.mainnet.silver_near_social_txs_parsed
WHERE
  post:main:text IS NOT NULL;
"""
