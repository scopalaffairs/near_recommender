# Query to remove duplicates from silver_near_social_txs_parsed table.

query = """
CREATE TABLE hive_metastore.sit.near_social_txs_clean AS (
-- Table to eliminate the duplicate transaction from silver_near_social_txs_parsed
WITH duplicates AS (
  SELECT
    block_date
    , signer_id
    , receipt_id
    , deposit
    , gas
    , account_object
    , widget
    , post
    , profile
    , graph
    , settings
    , badge
    , index
    , COUNT(*) AS tx_count
  FROM
    hive_metastore.mainnet.silver_near_social_txs_parsed
  WHERE
    method_name = 'set'
  GROUP BY 
    block_date
    , signer_id
    , receipt_id
    , deposit
    , gas
    , account_object
    , widget
    , post
    , profile
    , graph
    , settings
    , badge
    , index
  HAVING 
    tx_count > 1
)

, unique_txs AS (
  SELECT
    block_date
    , signer_id
    , receipt_id
    , deposit
    , gas
    , account_object
    , widget
    , post
    , profile
    , graph
    , settings
    , badge
    , index
    , COUNT(*) AS tx_count
  FROM
    hive_metastore.mainnet.silver_near_social_txs_parsed
  WHERE
    method_name = 'set'
  GROUP BY 
    block_date
    , signer_id
    , receipt_id
    , deposit
    , gas
    , account_object
    , widget
    , post
    , profile
    , graph
    , settings
    , badge
    , index
  HAVING 
    tx_count = 1
)

, removed_duplicates AS (
  SELECT
    block_date
    , signer_id
    , receipt_id
    , deposit
    , gas
    , account_object
    , widget
    , post
    , profile
    , graph
    , settings
    , badge
    , index
  FROM
    duplicates
  UNION ALL SELECT
    block_date
    , signer_id
    , receipt_id
    , deposit
    , gas
    , account_object
    , widget
    , post
    , profile
    , graph
    , settings
    , badge
    , index
  FROM
    unique_txs
)

  SELECT *
  FROM removed_duplicates
)
"""