Algorithms
=====================================

This section explains the different recommendation algorithms employed in the system.

Trending Users
================

The trending users algorithm appends the top trending users to the recommendation list. This algorithm is applied in the following cases:

- If the user is new (< 1 week, < 3 days)
- If the user is not active
- If the user was inactive for some period

Friends-of-Friends
===================

The friends-of-friends algorithm appends users who are friends-of-friends to the recommendation list. 
This algorithm is applied when the user is active.

Similar Posts
==============

The similar posts algorithm appends users who have made similar posts to the recommendation list. 
This algorithm is applied when the user has posted.

Similar Profile Tags
=====================

The similar profile tags algorithm appends users who have similar profile tags to the recommendation list. 
This algorithm is applied when the user has specified tags.


.. toctree::
    :maxdepth: 1

    source/near_recommender.src.models.rst
