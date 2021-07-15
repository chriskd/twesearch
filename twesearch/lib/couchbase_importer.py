import couchbase
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
import couchbase.subdocument as SD
from couchbase.durability import Durability
from couchbase.cluster import QueryOptions
from .util import create_stdout_logger
import logging

class CouchbaseImporter:
    def __init__(self, cb_uri, auth):
        self.cluster = Cluster(cb_uri, ClusterOptions(
            PasswordAuthenticator(auth['user'], auth['password'])))
        tweet_bucket = self.cluster.bucket('tweets')
        users_bucket = self.cluster.bucket('twitter_users')

        self.tweet_collection = tweet_bucket.default_collection()
        self.users_collection = users_bucket.default_collection()

        self.logger = create_stdout_logger('debug')
    
    def upsert_document(self, bucket, doc):
        try:
            if bucket == 'tweets':
                bucket_collection = self.tweet_collection
            elif bucket == 'users':
                bucket_collection = self.users_collection

            key = doc["id"]
            result = bucket_collection.upsert(key, doc)
            return result.cas
        except Exception as e:
            print(e)
    
    def upsert_documents(self, bucket, items):
        logging.info(f'Upserting {len(items)} items to {bucket} bucket')
        results = [self.upsert_document(bucket, item) for item in items]
        logging.info(f"{len(results)} items upserted")