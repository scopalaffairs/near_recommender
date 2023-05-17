Models
======

This section provides details about the machine learning models and algorithms used in the recommender system.


Trending Users
--------------

The trending users algorithm appends the top trending users to the recommendation list. This method is applied in the following cases:

- If the user is new (< 1 week, < 3 days)
- If the user is not active
- If the user was inactive for some period

.. automodule:: near_recommender.src.models.trending_users
   :members:
   :undoc-members:
   :show-inheritance:

Friends-of-Friends
------------------

The friends-of-friends algorithm appends users who are friends-of-friends to the recommendation list. 
This model is applied when the user is active.

.. automodule:: near_recommender.src.models.friends_friends
   :members:
   :undoc-members:
   :show-inheritance:

Similar Posts
-------------

The similar posts model appends users who have made similar posts to the recommendation list. 
This model is applied when the user has posted.

.. automodule:: near_recommender.src.models.similar_posts
   :members:
   :undoc-members:
   :show-inheritance:


Similar Profile Tags
--------------------

The similar profile tags algorithm appends users who have similar profile tags to the recommendation list. 
This algorithm is applied when the user has specified tags.

.. automodule:: near_recommender.src.models.similar_tags
   :members:
   :undoc-members:
   :show-inheritance:


.. toctree::
    :maxdepth: 1

    source/near_recommender.src.models.rst
