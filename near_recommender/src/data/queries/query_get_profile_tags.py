query = """
SELECT
  signer_id,
  profile:tags as profile,
  block_timestamp
FROM
  hive_metastore.mainnet.silver_near_social_txs_parsed
WHERE
  profile:tags IS NOT NULL;
"""
