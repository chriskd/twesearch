import logging
import datetime
import json
from decimal import Decimal


def defloat(results):
    return json.loads(json.dumps(results), parse_float=Decimal)

def add_timestamp_to_list_items(items):
    timestamp = datetime.datetime.now().isoformat()
    logging.info(f"adding timestamp {timestamp} to items")
    for item in items:
        item.update({'fetched_timestamp': timestamp})
    return items

def dedupe_tweets(tweets):
    logging.info(f"Starting with {len(tweets)} total tweets")
    unique_tweets = list({v['id']:v for v in tweets}.values())
    logging.info(f"Found {len(unique_tweets)} unique tweets")
    return unique_tweets     

def extract_expansions_and_tweets(results, dedupe=True):
    logging.info("Separating tweets, users and places")
    counts = {'total_tweets_count': 0, 'dedupe_tweets_count': 0}

    tweets = [i for i in results if 'text' in i.keys()]
    expanded_tweets = [t for i in [e['tweets'] for e in results if 'tweets' in e.keys()] for t in i if 'text' in t.keys() ]
    logging.info(f"Fetched {len(tweets)} from outer list and {len(expanded_tweets)} from Tweet expansions object")

    users = [i for i in results if 'username' in i.keys()]
    expanded_users = [t for i in [e['users'] for e in results if 'users' in e.keys()] for t in i]
    logging.info(f"Fetched {len(users)} from outer list and {len(expanded_users)} from Users expansion object")

    tweets = tweets + expanded_tweets
    if tweets:
        counts['total_tweets_count'] = sum([x['result_count'] for x in results if 'result_count' in x.keys()])
        tweets = add_timestamp_to_list_items(tweets)
        if dedupe:
            tweets = dedupe_tweets(tweets)
            counts['dedupe_tweets_count'] = len(tweets)
        #if defloat:
        #    tweets = defloat(tweets)

    users = users + expanded_users
    if users:
        users = add_timestamp_to_list_items(users)

    logging.info(f"Final stats: {len(tweets)} total tweets, {len(users)} total users")
    return {'tweets': tweets, 'users': users, 'counts': counts}