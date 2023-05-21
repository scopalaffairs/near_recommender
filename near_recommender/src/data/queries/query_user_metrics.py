# Query to create a table with different user metrics and calculate trending metric from them.

query = """
CREATE TABLE hive_metastore.sit.users_agg_metrics AS (
WITH likes_raw AS (
SELECT
  signer_id
  , receipt_id
  , SUBSTRING(index:like:key:path, 1, CHARINDEX('/', index:like:key:path) - 1) AS likee
  --, CHARINDEX('/', index:like:key:path, CHARINDEX('/', index:like:key:path) + 1) - CHARINDEX('/', index:like:key:path) - 1) AS xy
  , index:like:value:type AS type
  , block_date
FROM
  hive_metastore.sit.near_social_txs_clean
WHERE
  index:like:key:type = 'social'
)

, likes AS (
  SELECT
    signer_id
    , SUM(CASE WHEN type = 'like' THEN 1 ELSE 0 END) AS total_likes
    , SUM(CASE WHEN type = 'unlike' THEN 1 ELSE 0 END) AS unlikes
    , total_likes - unlikes AS likes
  FROM 
    likes_raw
  GROUP BY 
    signer_id
)
, likes_last_30d AS (
  SELECT
    signer_id
    , SUM(CASE WHEN type = 'like' THEN 1 ELSE 0 END) AS total_likes
    , SUM(CASE WHEN type = 'unlike' THEN 1 ELSE 0 END) AS unlikes
    , total_likes - unlikes AS likes
  FROM 
    likes_raw
  WHERE
    block_date > CURRENT_DATE - 30
  GROUP BY 
    signer_id
)

, likers AS (
  SELECT
    likee
    , SUM(CASE WHEN type = 'like' THEN 1 ELSE 0 END) AS total_likers
    , SUM(CASE WHEN type = 'unlike' THEN 1 ELSE 0 END) AS unlikers
    , total_likers - unlikers AS likers
  FROM 
    likes_raw
  GROUP BY 
    likee
)

, likers_last_30_days AS (
  SELECT
    likee
    , SUM(CASE WHEN type = 'like' THEN 1 ELSE 0 END) AS total_likers
    , SUM(CASE WHEN type = 'unlike' THEN 1 ELSE 0 END) AS unlikers
    , total_likers - unlikers AS likers
  FROM 
    likes_raw
  WHERE
    block_date > CURRENT_DATE - 30
  GROUP BY 
    likee
)

, likes_agg AS (
  SELECT
    l.signer_id
    , l.likes
    , lr.likers
    , likers / likes as like_ratio
  FROM
    likes l
  LEFT JOIN
    likers lr
  ON
    l.signer_id = lr.likee
)

, likes_last_30d_agg AS (
  SELECT
    l.signer_id
    , l.likes AS likes_30d
    , lr.likers AS likers_30d
    , likers_30d / likes_30d as like_ratio_30d
  FROM
    likes_last_30d l
  LEFT JOIN
    likers_last_30_days lr
  ON
    l.signer_id = lr.likee
)

, follows_raw AS (
  SELECT
    signer_id
    , receipt_id
    , graph:follow
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
    , explode(split(follow, ',')) AS follow_explo
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

, total_user_follows AS (
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

, following AS (
  SELECT
    signer_id
    , SUM (CASE type WHEN 'FOLLOW' THEN 1 ELSE 0 END) AS add_follows
    , SUM (CASE type WHEN 'UNFOLLOW' THEN 1 ELSE 0 END) AS unfollows
    , add_follows - unfollows AS following
  FROM
    total_user_follows
  GROUP BY 
    signer_id
)

, following_last_30d AS (
  SELECT
    signer_id
    , SUM (CASE type WHEN 'FOLLOW' THEN 1 ELSE 0 END) AS add_follows
    , SUM (CASE type WHEN 'UNFOLLOW' THEN 1 ELSE 0 END) AS unfollows
    , add_follows - unfollows AS following
  FROM
    total_user_follows
  WHERE
    block_date > CURRENT_DATE - 30
  GROUP BY 
    signer_id
)

, net_user_follows AS (
  SELECT
    signer_id
    , follows
    , SUM(CASE WHEN type = 'FOLLOW' THEN 1 END) as follows_count
    , SUM(CASE WHEN type = 'UNFOLLOW' THEN 1 END) as unfollows_count
    , CASE
      WHEN unfollows_count IS NULL THEN follows_count
      WHEN unfollows_count >= 1 THEN follows_count - unfollows_count
      END AS net_count
  FROM
    total_user_follows
  GROUP BY
    signer_id
    , follows
  ORDER BY
    signer_id
)

, net_user_follows_30d AS (
  SELECT
    signer_id
    , follows
    , SUM(CASE WHEN type = 'FOLLOW' THEN 1 END) as follows_count
    , SUM(CASE WHEN type = 'UNFOLLOW' THEN 1 END) as unfollows_count
    , CASE
      WHEN unfollows_count IS NULL THEN follows_count
      WHEN unfollows_count >= 1 THEN follows_count - unfollows_count
      END AS net_count
  FROM
    total_user_follows
  WHERE
    block_date > CURRENT_DATE - 30
  GROUP BY
    signer_id
    , follows
  ORDER BY
    signer_id
)

, followers AS (
  SELECT
    follows
    , COUNT(DISTINCT signer_id) AS followers
  FROM
    net_user_follows
  WHERE
    follows_count IS NOT NULL OR net_count >= 1
  GROUP BY 
    follows
)

, followers_last_30d AS (
  SELECT
    follows
    , COUNT(DISTINCT signer_id) AS followers
  FROM
    net_user_follows_30d
  WHERE
    follows_count IS NOT NULL OR net_count >= 1
  GROUP BY 
    follows
)

, follow_agg AS (
  SELECT
    g.signer_id
    , g.following
    , r.followers
    , r.followers / g.following AS follow_ratio
  FROM
    following g
  LEFT JOIN
    followers r
  ON
    g.signer_id = r.follows
)

, follow_last_30d_agg AS (
  SELECT
    g.signer_id
    , g.following AS following_30d
    , r.followers AS followers_30d
    , r.followers / g.following AS follow_ratio_30d
  FROM
    following_last_30d g
  LEFT JOIN
    followers_last_30d r
  ON
    g.signer_id = r.follows
)

, metrics_raw AS (
  SELECT
    signer_id
    , MIN(block_date) AS first_txs
    , DATEDIFF(CURRENT_DATE(), first_txs) AS address_age
    , MAX(block_date) AS last_txs
    , DATEDIFF(CURRENT_DATE(), last_txs) AS recency
    , CASE
        WHEN recency < 8 THEN 1
        ELSE 0
        END AS active_last_week
    , CASE
        WHEN recency < 31 THEN 1
        ELSE 0
        END AS active_last_month
    , COUNT(DISTINCT receipt_id) AS total_txs
    , COUNT(DISTINCT block_date) AS days_active
    , COUNT(post) AS posts
    , posts / days_active AS posts_per_day
    , CASE posts WHEN 0 THEN 0 ELSE 1 END AS user_has_posted
    , COUNT(profile:tags) AS tags_update
    , CASE tags_update WHEN 0 THEN 0 ELSE 1 END AS user_has_tags
    , SUM(gas)/POW(10,12) AS TGas
    , COUNT(widget) AS widget_txs
    , CASE widget_txs WHEN 0 THEN 0 ELSE 1 END AS used_widget
    , COUNT(profile) AS profile_updates
    , CASE profile_updates WHEN 0 THEN 0 ELSE 1 END AS updated_profile
    , COUNT(graph) AS graph_txs
    , CASE graph_txs WHEN 0 THEN 0 ELSE 1 END AS used_graph
    , COUNT(settings) AS settings_txs
    , CASE settings_txs WHEN 0 THEN 0 ELSE 1 END AS used_settings
    , COUNT(badge) AS badge_txs
    , CASE badge_txs WHEN 0 THEN 0 ELSE 1 END AS used_badge
    , COUNT(index) AS index_txs
    , CASE index_txs WHEN 0 THEN 0 ELSE 1 END AS used_index
  FROM
    hive_metastore.sit.near_social_txs_clean
  GROUP BY 
    signer_id
)

, last_month_activity AS (
SELECT
    signer_id
    , COUNT(DISTINCT receipt_id) AS total_txs
    , COUNT(DISTINCT block_date) AS days_active
    , COUNT(post) AS posts
    , posts / days_active AS posts_per_day
  FROM
    hive_metastore.sit.near_social_txs_clean
  WHERE
    block_date > CURRENT_DATE - 30
  GROUP BY 
    signer_id
)

, comments_30d AS (
SELECT 
  SUBSTRING(post:comment:item:path, 0, CHARINDEX('/', post:comment:item:path) - 1) AS comment_to
  , COUNT (*) AS comments_30d
FROM 
  hive_metastore.sit.near_social_txs_clean 
WHERE 
  post:comment:item:type = 'social'
  AND block_date > CURRENT_DATE - 30
  AND SUBSTRING(post:comment:item:path, 0, CHARINDEX('/', post:comment:item:path) - 1) != signer_id
GROUP BY
  comment_to
)

, agg_metrics AS (
SELECT
  m.signer_id
  , m.address_age
  , m.recency
  , m.user_has_posted
  , m.user_has_tags
  , m.active_last_week
  , m.active_last_month
  , m.days_active
  , m.posts
  , m.posts_per_day
  , m.days_active / m.address_age * 100 AS perc_active_days
  , lm.days_active AS days_active_30d
  , lm.posts AS posts_30d
  , lm.posts / 30 AS post_per_day_30d 
  , lm.days_active / 30 * 100 AS perc_active_days_30d
  , f.following
  , f.followers
  , f.follow_ratio
  , ft.following_30d
  , ft.followers_30d
  , ft.follow_ratio_30d
  , l.likes
  , l.likers
  , l.like_ratio
  , lt.likes_30d
  , lt.likers_30d
  , lt.like_ratio_30d
  , c.comments_30d
FROM
  metrics_raw m
LEFT JOIN
  last_month_activity lm
ON
  m.signer_id = lm.signer_id
LEFT JOIN
  follow_agg f
ON 
  m.signer_id = f.signer_id
LEFT JOIN
 follow_last_30d_agg ft
ON
  m.signer_id = ft.signer_id
LEFT JOIN
  likes_agg l
ON
  m.signer_id = l.signer_id
LEFT JOIN
  likes_last_30d_agg lt
ON
  m.signer_id = lt.signer_id
LEFT JOIN
  comments_30d c
ON
  m.signer_id = c.comment_to
ORDER BY
  followers DESC
)

SELECT
  *
  , comments_30d + likers_30d + followers_30d AS engagement_30d
  , DOUBLE(comments_30d + 0.5*likers_30d + 0.1*followers_30d) AS engagement_weighted_30d
  , posts_30d + likes_30d + following_30d AS activity_30d
  , DOUBLE(posts_30d + 0.5*likes_30d + 0.1*following_30d) AS activity_weighted_30d
  , engagement_weighted_30d / activity_weighted_30d AS trending_metric
FROM
  agg_metrics
ORDER BY
  followers DESC
)
"""