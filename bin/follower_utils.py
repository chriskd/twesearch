import click
import logging
import csv
import yaml

from twesearch.lib import neo4j_importer
from twesearch.lib.util import format_tweets_for_neo4j, add_campaign
from twesearch.lib import couchbase_importer
from twesearch import Twesearch

logging.disable(logging.DEBUG)

@click.command()
@click.option('-u', '--username')
@click.option('-i', '--in-file')
@click.option('-s', '--sleep', type=int)
@click.option('-r', '--followers', is_flag=True, default=False)
@click.option('-g', '--following', is_flag=True, default=False)
@click.option('-c', '--csv', '_csv', is_flag=True)
@click.option('-y', '--yolo', is_flag=True)
@click.option('-nn', '--no-neo4j', 'neo4j_flag', is_flag=True, default=False)
@click.option('-nc', '--no-couchbase', 'cb_flag', is_flag=True, default=False)
def main(username, followers, following,neo4j_flag,cb_flag, in_file, _csv, sleep, yolo):

    with open (r'config.yaml') as f:
        GLOBAL_CONFIG = yaml.load(f, Loader=yaml.FullLoader)

    if not neo4j_flag:
        neo4j = neo4j_importer.Neo4jImporter(neo4j_uri=GLOBAL_CONFIG['neo4j_uri'], db_name=GLOBAL_CONFIG['neo4j_dbname'], 
                                                    auth={'user': GLOBAL_CONFIG['neo4j_user'], 'password': GLOBAL_CONFIG['neo4j_pw']},log=True)
    if not cb_flag:
        couchbase = couchbase_importer.CouchbaseImporter(cb_uri=GLOBAL_CONFIG['cb_uri'], auth={'user': GLOBAL_CONFIG['cb_user'], 
                                                                                                'password': GLOBAL_CONFIG['cb_pw']})

    tv2 = Twesearch(log=True, log_level='info')

    def get_follows(username):
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
                if followers_count > 300 and not yolo:
                    val = input(f'Warning: {username} has a large number of followers: {followers_count}. Proceed? y/n\n')
                    if val == 'n':
                        quit()

                print(f'Fetching followers')
                follower_users = tv2.get_followers(user_id, expansions='', max_results=500000, sleep=sleep)['users']
                print(f'Fetched {len(follower_users)} followers')
                follower_ids = [follower['id'] for follower in follower_users]

                user['followers'] = follower_ids
                users = users + follower_users

            if following:
                following_count = user["public_metrics"]["following_count"]

                print(f'{username} is following {following_count} users')
                if following_count > 300 and not yolo:
                    val = input(f'Warning: {username} has a large number of following: {following_count}. Proceed? y/n\n')
                    if val == 'n':
                        quit()

                print(f'Fetching following')
                following_users = tv2.get_following(user_id, expansions='', max_results=500000, sleep=sleep)['users']
                print(f'Fetched {len(following_users)} following')
                following_ids = [following['id'] for following in following_users]

                user['following'] = following_ids
                users = users + following_users
            
            users.append(user)
            header_fields = ['created_at', 'description', 'entities', 'id', 'location', 'name', 'pinned_tweet_id', 
            'profile_image_url', 'protected', 'public_metrics', 'url', 'username', 'verified', 'fetched_timestamp', 'withheld']
            if not cb_flag:
                print(f'Inserting into couchbase')
                couchbase.upsert_documents('users', users)
                user = [user]
                couchbase.upsert_documents('users', user)
            if not neo4j_flag:
                print(f'Inserting into neo4j')
                neo4j.insert('users', users)
            if _csv and following:
                print('Writing following CSV')
                with open(username + '_following.csv', 'w') as following_csv:
                    writer = csv.DictWriter(following_csv, fieldnames = header_fields, 
                                            delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                    writer.writeheader()
                    writer.writerows(following_users)
            if _csv and followers:
                print('Writing follower CSV')
                with open(username + '_followers.csv', 'w') as follower_csv:
                    writer = csv.DictWriter(follower_csv, fieldnames = header_fields, 
                                            delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                    writer.writeheader()
                    writer.writerows(follower_users)
    if in_file:
        with open(in_file) as i_f:
            usernames = i_f.read().splitlines()
    else:
        usernames = [username]
    
    for username in usernames:
        get_follows(username)

if __name__ == '__main__':
    main()