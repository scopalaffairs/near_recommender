query = """
SELECT
  signer_id,
  profile:tags,
  block_timestamp
FROM
  hive_metastore.sit.near_social_txs_clean
WHERE
  profile:tags IS NOT NULL
"""
