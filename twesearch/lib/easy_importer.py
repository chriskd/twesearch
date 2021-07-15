from twesearch.lib import neo4j_importer
from twesearch.lib.util import format_tweets_for_neo4j, add_campaign
from twesearch.lib import couchbase_importer

neo4j_importer = neo4j_importer.Neo4jImporter(
                                    neo4j_uri='bolt://10.124.0.3:7687',
                                    db_name='twitter', 
                                    auth={'user': 'neo4j', 
                                            'password': '***REMOVED***'},
                                    log=True)

couchbase_importer = couchbase_importer.CouchbaseImporter(
                                    cb_uri='couchbase://10.124.0.3', 
                                    auth={'user': 'admin', 
                                            'password': '***REMOVED***'})

def easy_import(items):
    tweets = items['tweets']
    users = items['users']

    if len(tweets) > 0:                                                                        

        couchbase_importer.upsert_documents('tweets', tweets)

        neo4j_tweets = format_tweets_for_neo4j(tweets, users)
        neo4j_importer.insert('tweets', neo4j_tweets)
    
    if len(users) > 0:
        couchbase_importer.upsert_documents('users', users)

        neo4j_importer.insert('users', users)