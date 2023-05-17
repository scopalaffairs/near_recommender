Commands
========

This section provides details about the functions the models use for their recommendations.

Along with the main recommender functions, we expose commands that can be scheduled for repeating updates.

- ``get_recommendations_per_user()``

See ``near_recommender/src/process_recommendations.py`` for reference on how to use the main logic, pushing updated JSON objects for each user to an S3 bucket.

- ``update_corpus()``

The ``update_corpus`` command is responsible for updating the locally stored Large Language Model, which is defined in the ``path`` variable within the ``similar_posts`` module.

Each update in the pooled word embeddings model triggers an increment in its version number.

The function ``load_pretrained_model``, which is used in ``similar_posts``, handles loading the latest version.

.. admonition:: Machine Learning Model Management
   :class: important
   
   Feel free to optimize this with your own version management for machine learning models.


.. toctree::
    :maxdepth: 1

    source/near_recommender.src.features.rst
