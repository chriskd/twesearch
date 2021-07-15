import logging
import sys

def format_tweets_for_neo4j(tweets, users):
    print(f'Formatting {len(tweets)} for neo4j ingestion')
    neo4j_tweets = []
    for t in tweets:
        user = [u for u in users if u['id'] == t['author_id']][0]
        neo4j_tweet = {}
        for k,v in t.items():
          neo4j_tweet['tweet_' + k] = v
        for k,v in user.items():
          neo4j_tweet['tweet_author_' + k] = v
        neo4j_tweets.append(neo4j_tweet)
    print(f'Formatted {len(neo4j_tweets)} for neo4j ingestion')
    return neo4j_tweets

def ghetto_split(list_, chunk_size=100):
    """
    Utility function to split a list into a list of lists of size chunk_size
    """
    logging.debug(f"Splitting list of {len(list_)} length, chunk size = {chunk_size}")
    split_lists = []
    for i in range(0,len(list_),chunk_size):
        split_lists.append(list_[i:i+chunk_size])
    logging.debug(f"List has been split into {len(split_lists)} lists. Total num of elements in split lists is {sum([len(i) for i in split_lists])}")
    return split_lists

def add_campaign(items, campaign):
    for item in items:
        item.update({"camp_id": campaign})
    return items     

def create_stdout_logger(log_level):
    fmt="%(levelname)s:%(module)s:%(funcName)s():%(lineno)i: %(message)s"

    if log_level == "debug":
        log_level = logging.DEBUG
    elif log_level == "info":
        log_level = logging.INFO
    elif log_level == "warn":
        log_level = logging.WARN
    
    logging.basicConfig(stream=sys.stdout,level=log_level,format=fmt)
    logger = logging.getLogger()
    formatter = logging.Formatter(fmt)
    logger.handlers[0].setFormatter(formatter)
    return logger