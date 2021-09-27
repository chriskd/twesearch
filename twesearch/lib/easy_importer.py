from twesearch.lib import neo4j_importer
from twesearch.lib.util import format_tweets_for_neo4j, add_campaign
from twesearch.lib import couchbase_importer
import logging
import yaml

logging.disable(logging.DEBUG)


with open (r'config.yaml') as f:
    GLOBAL_CONFIG = yaml.load(f, Loader=yaml.FullLoader)

neo4j_importer = neo4j_importer.Neo4jImporter(
                                    neo4j_uri=GLOBAL_CONFIG['neo4j_uri'],
                                    db_name=GLOBAL_CONFIG['neo4j_dbname'], 
                                    auth={'user': GLOBAL_CONFIG['neo4j_user'], 
                                            'password': GLOBAL_CONFIG['neo4j_pw']},
                                    log=True)

couchbase_importer = couchbase_importer.CouchbaseImporter(
                                    cb_uri=GLOBAL_CONFIG['cb_uri'], 
                                    auth={'user': GLOBAL_CONFIG['cb_user'],
                                            'password': GLOBAL_CONFIG['cb_pw']})

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