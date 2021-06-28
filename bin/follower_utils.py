from optparse import OptionParser
import click
import logging

from twesearch.lib import neo4j_importer
from twesearch.lib.util import format_tweets_for_neo4j, add_campaign
from twesearch.lib import couchbase_importer
from twesearch import Twesearch

logging.disable(logging.DEBUG)

@click.command()
@click.option('-u', '--username')
@click.option('-r', '--followers', is_flag=True, default=False)
@click.option('-g', '--following', is_flag=True, default=False)
@click.option('-n', '--neo4j', 'neo4j_flag', is_flag=True, default=True)
@click.option('-c', '--couchbase', 'cb_flag', is_flag=True, default=True)
def main(username, followers, following,neo4j_flag,cb_flag):
    neo4j = neo4j_importer.Neo4jImporter(neo4j_uri='bolt://10.124.0.3:7687', db_name='twitter', 
                                                    auth={'user': 'neo4j', 'password': '***REMOVED***'},log=True)
    couchbase = couchbase_importer.CouchbaseImporter(cb_uri='couchbase://10.124.0.3', auth={'user': 'admin', 
                                                                                                'password': '***REMOVED***'})
    print('Fetching:')
    if followers: print('Followers') 
    if following: print('Following')
    print(f'For {username}. Buckle up partner')

    tv2 = Twesearch(log=True, log_level='info')
    
    user_id = tv2.username_to_id(username)
    if user_id:
        user = tv2.get_users([user_id], expansions='')['users'][0]
        users = []
        if followers:
            followers_count = user["public_metrics"]["followers_count"]

            print(f'{username} has {followers_count} followers')
            if followers_count > 300:
                val = input(f'Warning: {username} has a large number of followers: {followers_count}. Proceed? y/n\n')
                if val == 'n':
                    quit()

            print(f'Fetching followers')
            follower_users = tv2.get_followers(user_id, expansions='')['users']
            print(f'Fetched {len(follower_users)} followers')
            follower_ids = [follower['id'] for follower in follower_users]

            user['followers'] = follower_ids
            users = users + follower_users

        if following:
            following_count = user["public_metrics"]["following_count"]

            print(f'{username} is following {following_count} users')
            if following_count > 300:
                val = input(f'Warning: {username} has a large number of following: {following_count}. Proceed? y/n\n')
                if val == 'n':
                    quit()

            print(f'Fetching following')
            following_users = tv2.get_following(user_id, expansions='')['users']
            print(f'Fetched {len(following_users)} following')
            following_ids = [following['id'] for following in following_users]

            user['following'] = following_ids
            users = users + following_users
        
        users.append(user)

        if cb_flag:
            print(f'Inserting into couchbase')
            couchbase.upsert_documents('users', users)
        if neo4j_flag:
            print(f'Inserting into neo4j')
            neo4j.insert('users', users)

if __name__ == '__main__':
    main()