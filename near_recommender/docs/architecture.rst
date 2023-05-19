System
------

This section describes the overall architecture of the recommender system, including the flow of data and the different recommendation algorithms utilized.

Recommendation system logic:
    - If the user is new (< 1 week, < 3 days): appends trending users
    - If the user is not active: appends trending users
    - If the user is active: appends friends-of-friends
    - If the user has a tag: appends tag similarity
    - If the user has posted: appends post similarity
    - If the user was inactive for some period: appends trending users

.. image:: images/Architecture.png
    :alt: Recommender System Architectural Overview
    :width: 800px
    :align: center
