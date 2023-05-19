# NEAR Recommender System

**Index**

1. [NEAR Social](#near-social)
2. [Documentation](#documentation)
3. [Introduction](#introduction)
4. [Task](#task)
5. [Result](#result)
6. [Technical details](#technical-details)
7. [Methodologies](#methodologies)
   - [SQL queries](#sql-queries)
   - [Notebooks](#notebooks)
8. [Widget](#widget)
9. [Visualization](#visualization)
10. [Authors](#authors)


This repository contains the files used for the Capstone Project "NEAR Social Recommender - A recommender system for an on-chain social network" of the Data Science Bootcamp , Batch 03/2023 at Constructor.

This project was done in collaboration with [Pagoda](https://www.pagoda.co/), the software development company behind the NEAR Blockchain Operating System.

## NEAR Social

## Documentation

[Documentation](https://scopalaffairs.github.io/near_recommender/)

### Introduction

NEAR Social is a blockchain-based social network where users log in with their NEAR wallet address. All user actions, such as posting, following, liking, and updating their profile, are recorded on the public ledger as blockchain transactions. Users own their data, and developers can create permissionless open-source apps, known as widgets, to expand the platform's capabilities.

### Task

Our objective was to develop a user recommendation system that fosters network growth by connecting users with similar interests. To achieve this, we designed a system that utilizes on-chain data for each user. We employed four distinct recommendation algorithms, as illustrated in the architectural overview below:

- Top trending users
- Friends of friends
- Tag similarity 
- Post similarity

![Recommender System Architectural Overview](near_recommender/docs/images/Architecture.png)

### Result

This recommender system is available through a widget on [near.org](https://near.org/)

## Technical details

This project used the on-chain data on the NEAR blockchain via the Databricks instance of Pagoda. We created SQL queries and tables as well as Data Science Notebooks.

## Methodologies

Among others, we explored the given datasets with the following methods:

- Friends of friends
  - XGBoost
  - RandomForest
- Trending users
  - NetworkX
  - Louvain community detection
- Tag/Post Similarity
  - Natural Language Processing, Cosine Similarity
  - Pooled word embeddings on Large Transformer Model, Cosine Similarity
- Hyperlink-Induced-Topic-Search (HITS) Algorithm
  - Graphs for visualization and exploration

### SQL queries

We created our own SQL tables using existing parsed tables to process the data to our needs. These tables include:

- **near_social_txs_clean**: transactions within the social.near contract without duplicates
- **graph_follows**: table showing users and follows in the form of graph edges
- **users_agg_metrics**: account and social network metrics by user

These tables can be found in the `sit` schema inside Databricks.

### Notebooks

Several notebooks inside and outside Databricks have been created to implement the different recommender algorithms. These can be found under `near_recommender/notebooks` inside this repository.


## Widget

The recommender system is going to be implemented as a widget.


## Visualization

Unveiling the web of network connections and community clusters, several iterations of visual interfaces gave us a comprehensive understanding of user relationships, facilitating trending user recommendations and fine tuning the models.

![Visualization of the near social network and its clusters](near_recommender/docs/images/near_network_graph_still.png)


## Documentation

The documentation is hosted via Github Pages from the branch `docs`, folder `/docs`.
To work smoothly with Github, we need an empty `.nojekyll` file in the compiled docs directory `(project_root)/docs`.

To build the docs, we can use the make script in the documentation source directory, `near_recommender/docs/`.

To build the docs from scratch, you need to have a Java runtime on your local machine, and have the packages virtualenvironment activated. See `Development` for further steps.


## Development

1. Package Management

This package is managed by `Poetry` ![Python Poetry Package Management](https://python-poetry.org/docs/)

You need the tool installed to interact with its interface.

Basic commands:

`poetry shell`

Activates a virtualenv for this project.

`poetry install`

Installs the requirements into the virtualenv.

`poetry add/remove <package>`

Installs/removes packages. Poetry handles dependency version management automatically. Keep in mind to let the tool handle dependencies by using the above commands, instead of manually changing versions.

Specific versions for a package can be installed by adding the version, see: https://python-poetry.org/docs/dependency-specification

`poetry update`

Updates the entire project.

`poetry build`
Builds a wheel from the package.

For further commands, consult poetry's docs.

2. Python Version

We are depending on Databricks LTS support for the Python version, check `pyproject.toml` for further information. 

### Authors

[Agustin Rojo Serrano](https://www.linkedin.com/in/rojoserrano/)

[Christian Kühner](https://www.linkedin.com/in/christian-k%C3%BChner-9295301b1/)

[Daniel Herrmann](https://www.linkedin.com/in/daniel-herrmann/)
