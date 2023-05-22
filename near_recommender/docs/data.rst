Data
====

This section provides details about the queries used to feed the models.


SQL queries
-----------

Remove duplicates query
^^^^^^^^^^^^^^^^^^^^^^^

This query is used to remove duplicates from ``hive_metastore.mainnet.silver_near_social_txs_parsed`` table.

The query utilizes two Common Table Expressions (CTEs) to handle transactions:

- ``duplicates`` This CTE captures all transactions that are duplicated, along with a count indicating how many times each transaction appears.
- ``unique_txs`` This CTE contains only the unique transactions.

By merging these two CTEs, we create a new table without any duplicates. The resulting table is saved as ``hive_metastore.sit.near_social_txs_clean``.


Graph table query
^^^^^^^^^^^^^^^^^

Query to create an edge table for the graph representation of the social network.

Each row of this table represents a follow transaction with:

- signer_id
- user followed
- type (FOLLOW or UNFOLLOW)
- date 

First step is to parse the ``graph:follow`` argument from the ``hive_metastore.mainnet.silver_near_social_txs_parsed`` table and filter non-null, non-empty and non-failed transactions as represented by the ``WHERE`` clause on the first CTE.

Then, two different CTEs extract the followed user name on two different scenarios:

- *'single_user_follows'* extracts the substring that contains the user name. In this case, each transaction represents following one single user
- ``batch_user_follows`` uses the same logic to extract the user name but uses first an explode to deal with multiple follows in a single transaction. This was enabled by a batch following widget.

By merging the two CTEs above, we obtain a table with the edges of the graph representing the following connections in the social network. This table is saved as ``hive_metastore.sit.graph_follows``.


User metrics query
^^^^^^^^^^^^^^^^^^

Query to create a table with different user metrics and calculate trending metric from them.

Metrics can be divided into 3 categories:

- Metrics directly aggregated from the ``hive_metastore.sit.near_social_txs_clean`` table. These include 10 all-time metrics and 4 metrics for the past 30 days. All of them are calculated in the CTEs ``metrics_raw`` and ``last_month_activity``.

- Metrics that need additional CTES for their calculation:

    - Likes are calculated parsing the ``index:like`` argument from the ``hive_metastore.mainnet.silver_near_social_txs_parsed`` table and agreggating for the signer:id and the likee separately. Additionally, both metrics are calculated for the past 30 days.

    - Follows are calculated following the same structure as for the graph table query (see section above). When calculating the total followers and following for each user, separate methods have been used, empirically testing them with data from the social network. There is some slight mismatch which is thought to be caused by some missing transactions, but the final result is accurate enough (Max. 3% error on one user from the top 10 most followed accounts).

    - Comments for the past 30 days are calculated parsing the ``post:comment:item`` argument from the ``hive_metastore.mainnet.silver_near_social_txs_parsed`` table.

- Engagement, activity and trending metrics. These are calculated based on the previous categories, with the following definitions:

    - Engagement is calculated as the sum of comments, likers and followers in the past 30 days. There is a weighted variant using 1, 0.5 and 0.1 weights respectively.

    - Activity is calculated as the sum of posts, likes and followings in the past 30 days. There is a weighted variant using 1, 0.5 and 0.1 weights respectively.

    - The treding metric is defined as the ratio between weighted engagement and weighted activity with the intention of selecting the users which create the most appealing content for the network.



.. toctree::
    :maxdepth: 1

    source/near_recommender.src.data.rst