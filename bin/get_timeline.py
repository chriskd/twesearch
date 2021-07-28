import click
import logging
import json

from twesearch.lib import neo4j_importer
from twesearch.lib.util import format_tweets_for_neo4j, add_campaign
from twesearch.lib import couchbase_importer
from twesearch import Twesearch

logging.disable(logging.DEBUG)

@click.command()
@click.option('-u', '--username')
@click.option('-i', '--in-file')
@click.option('-n', '--neo4j', 'neo4j_flag', is_flag=True, default=True)
@click.option('-c', '--couchbase', 'cb_flag', is_flag=True, default=True)
def main(username, neo4j_flag,cb_flag, in_file):
    neo4j = neo4j_importer.Neo4jImporter(neo4j_uri='bolt://10.124.0.3:7687', db_name='twitter', 
                                                    auth={'user': 'neo4j', 'password': '***REMOVED***'},log=True)
    couchbase = couchbase_importer.CouchbaseImporter(cb_uri='couchbase://10.124.0.3', auth={'user': 'admin', 
                                                                                                'password': '***REMOVED***'})
    tv2 = Twesearch(log=True, log_level='info')
    def get_timeline(user_id):
        print(f'Fetching timeline tweets for {username}. Buckle up partner')
        with open('quota.json') as quota_file:
            quota = json.load(quota_file)
        
        quota_used = quota['quota_used']
        quota_max = quota['quota_max']
        print(f'Quota used: {quota_used}. Remaining calls: {quota_max - quota_used} out of {quota_max}')

        results = tv2.get_users_timeline_tweets(user_id)
        tweets_count = results['counts']['total_tweets_count']
        tweets = results['tweets']
        users = results['users']

        if tweets:
            quota_used += tweets_count
            print(f'Fetched {tweets_count}, new quota used: {quota_used}. Remaining calls: {quota_max - quota_used} out of {quota_max}')
            quota.update({'quota_used': quota_used})

            with open('quota.json', 'w') as quota_file:
                quota_file.write(json.dumps(quota))
            print('Inserting tweets')

            couchbase.upsert_documents('tweets', tweets)

            neo4j_tweets = format_tweets_for_neo4j(tweets, users)
            neo4j.insert('tweets', neo4j_tweets)
        
        if users:
            print(f'Fetched {len(users)}, Inserting users.')
            couchbase.upsert_documents('users', users)
            neo4j.insert('users', users)
    
    if in_file:
        with open(in_file) as i_f:
            usernames = i_f.read().splitlines()
        user_ids = [tv2.username_to_id(username) for username in usernames]
    else: 
        user_ids = tv2.username_to_id(username)
    
    for user_id in user_ids:
        get_timeline(user_id)
    
if __name__ == '__main__':
    main()