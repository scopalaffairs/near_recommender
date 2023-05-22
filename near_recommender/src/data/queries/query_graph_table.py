# Query to create an edge table for the graph representation of the social network.
# Each row of this table represents a follow transaction with:
# - signer_id
# - user followed
# - type (FOLLOW or UNFOLLOW)
# - date

query = """
CREATE TABLE hive_metastore.sit.graph_follows AS (
  WITH follows_raw AS(
    SELECT
      signer_id
      , graph:follow
      , receipt_id
      , account_object
      , block_date
    FROM
      hive_metastore.sit.near_social_txs_clean
    WHERE
      graph:follow IS NOT NULL
      AND graph:follow NOT IN ('{}', '{"*":""}') -- clean from empty and failed transactions
  )

  , single_user_follow AS (
    SELECT
      signer_id
      , SUBSTRING(follow, CHARINDEX('"', follow) + 1, CHARINDEX('"', follow, CHARINDEX('"', follow) + 1) - CHARINDEX('"', follow) - 1) AS follows
      , CASE
        WHEN CONTAINS(follow, 'null') THEN 'UNFOLLOW'
        ELSE 'FOLLOW'
        END AS type
      , block_date
    FROM
      follows_raw
    WHERE
      CONTAINS(follow, ',') IS FALSE
  )

  , batch_user_follow AS ( --coming from a batch following widget
    SELECT
      signer_id
      , EXPLODE(SPLIT(follow, ',')) AS follow_explo
      , SUBSTRING(follow_explo, CHARINDEX('"', follow_explo) + 1, CHARINDEX('"', follow_explo, CHARINDEX('"', follow_explo) + 1) - CHARINDEX('"', follow_explo) - 1) AS follows
      , CASE
        WHEN CONTAINS(follow_explo, 'null') THEN 'UNFOLLOW'
        ELSE 'FOLLOW'
        END AS type
      , block_date
    FROM
      follows_raw
    WHERE
      CONTAINS(follow, ',')
  )

  SELECT
    signer_id
    , follows
    , type
    , block_date
  FROM
    single_user_follow
  UNION ALL SELECT
    signer_id
    , follows
    , type
    , block_date
  FROM
    batch_user_follow
)
"""
