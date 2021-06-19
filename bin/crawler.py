#!/usr/bin/env python
from countdown import countdown
import logging
import json
from random import randrange,choice
import yaml
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, date

from twesearch.lib import neo4j_importer
from twesearch.lib.util import format_tweets_for_neo4j, add_campaign
from twesearch.lib import couchbase_importer
from twesearch import Twesearch

CAMPAIGN = "electionfraud-06-2021"
logging.disable(logging.DEBUG)
neo4j_importer = neo4j_importer.Neo4jImporter(neo4j_uri='bolt://10.124.0.3:7687', db_name='twitter', 
                                                auth={'user': 'neo4j', 'password': '***REMOVED***'},log=True)
couchbase_importer = couchbase_importer.CouchbaseImporter(cb_uri='couchbase://10.124.0.3', auth={'user': 'admin', 
                                                                                            'password': '***REMOVED***'})
tv2 = Twesearch(log=True, log_level='warn')

with open (r'config.yaml') as f:
    GLOBAL_CONFIG = yaml.load(f, Loader=yaml.FullLoader)
    TIMEOUT_MINUTES = GLOBAL_CONFIG['timeout_minutes']
    MAX_RESULTS = GLOBAL_CONFIG['max_results']

with open('queries.yaml') as f:
    queries = yaml.full_load(f)

print(f'Loaded {len(queries)} queries')

start_day_of_month = 5

while True:

    with open('quota.json') as quota_file:
        quota = json.load(quota_file)

    quota_max = quota['quota_max']

    today = str(date.today())
    quota_start_date = datetime.fromisoformat(quota['quota_start_date'])
    quota_end_date = quota_start_date + relativedelta(months=1)

    if datetime.now() > quota_end_date:
        quota_used = 0
        quota.update({'quota_start_date': today})
        with open('quota.json', 'w') as quota_file:
            quota_file.write(json.dumps(quota))
    else:
        quota_used = quota['quota_used']

    quota_remaining_timedelta = quota_end_date - datetime.now()
    quota_remaining_seconds = quota_remaining_timedelta.total_seconds()
    quota_remaining_iterations = int(quota_remaining_seconds / (60 * TIMEOUT_MINUTES))
    
    adaptive_max_results = int((quota_max - quota_used) / quota_remaining_iterations)

    query = choice(queries)

    print(f'''
    query: {query['query']}
    quota_used: {quota_used}
    quota remaining: {quota_max - quota_used}
    current quota start: {quota_start_date}
    current quota end: {quota_end_date}
    quota remaining timedelta: {quota_remaining_timedelta}
    quota remaining seconds: {quota_remaining_seconds}
    quota remaining iterations: {quota_remaining_iterations}
    adaptive max results: {adaptive_max_results}
    ''')

    since_id = query['since_id']

    if since_id:
        print(f'Found since_id, using {since_id}')
        query_args = {'since_id': since_id}
    else:
        query_args = {}
        
    results = tv2.search_tweets(query["query"], max_results = adaptive_max_results, other_query_args = query_args) 
    
    tweets_count = results['counts']['total_tweets_count']
    tweets = results['tweets']
    users = results['users']
    
    if len(tweets) > 0:                                                                        
        quota_used += tweets_count

        print('\r\r')
        print(f'Fetched {tweets_count}, new quota used: {quota_used}. Remaining calls: {quota_max - quota_used} out of {quota_max}')
        print('\r\r')

        tweets = add_campaign(tweets, CAMPAIGN)
        couchbase_importer.upsert_documents('tweets', tweets)
        
        neo4j_tweets = format_tweets_for_neo4j(tweets, users)
        neo4j_importer.insert('tweets', neo4j_tweets)
        
    if len(users) > 0:
        users = add_campaign(users, CAMPAIGN)
        couchbase_importer.upsert_documents('users', users)
        neo4j_importer.insert('users', users)

    if tweets:
        max_tweet_id = str(max([int(t['id']) for t in tweets]))
    else:
        continue

    print(f"Setting since_id to {max_tweet_id} for query {query['query']}")
    query.update({'since_id': max_tweet_id})

    print(f"Sleeping for {TIMEOUT_MINUTES} mins and writing updated since_ids to json")

    with open('queries.yaml', 'w') as queries_file:
        yaml.dump(queries, queries_file, sort_keys=True)

    quota.update({'quota_used': quota_used})
    with open('quota.json', 'w') as quota_file:
        quota_file.write(json.dumps(quota))
    countdown(mins=TIMEOUT_MINUTES,secs=00)    
