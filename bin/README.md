# twesearch/bin

Most of these depend on a 'config.yaml' file.

* add_query.py: Convience script to make it easier to add new twitter queries to the queries.yaml file
* crawler.py: Continuously query Twitter at the specified interval. Will randomly select a query term from the queries.yaml file. Will automatically adjust fetch quantity so as not to exceed the specified API limit per month. Imports results in to Couchbase and Neo4J
* follower_utils.py: For a given user, fetch all accounts they're following, and/or accounts that are following them. Can import into Couchbase, Neo4J, and/or a CSV file. 
* get_timeline.py: For a given user, fetch their timeline. Can import into Couchbase and/or Neo4j
