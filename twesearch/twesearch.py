from searchtweets import gen_request_parameters, load_credentials, collect_results
import tweepy
import logging
from pprint import pprint

from twesearch.lib.util import ghetto_split, create_stdout_logger
from twesearch.lib.tweet_util import extract_expansions_and_tweets, gen_request

EXPANSIONS = "entities.mentions.username,in_reply_to_user_id,author_id,geo.place_id,\
            referenced_tweets.id.author_id,referenced_tweets.id"
USER_FIELDS = "created_at,description,entities,id,location,name,pinned_tweet_id,\
            profile_image_url,protected,public_metrics,url,username,verified,withheld"
PLACE_FIELDS = "contained_within,country,country_code,full_name,geo,id,name,place_type"
TWEET_FIELDS = "author_id,text,context_annotations,conversation_id,created_at,entities,geo,\
            in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,withheld"

class Twesearch:

    def __init__(self,log=False, log_level="info", output_format='m'):
        if log:
            self.logger = create_stdout_logger(log_level)

        self.output_format = output_format

        self.search_args = load_credentials("~/.twitter_keys.yaml",
                                       yaml_key="search_tweets_v2",
                                       env_overwrite=False)
        self.search_args['output_format'] = self.output_format

        self.tweepy = tweepy.API(tweepy.AppAuthHandler(bearer_token=self.search_args['bearer_token']), 
                                wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def return_search_args(self):
        return self.search_args

    def return_tweepy_api(self):
        return self.tweepy
    
    def username_to_id(self, username):
        logging.info(f"Translating {username} to user ID")
        self.search_args['output_format'] = 'm'
        user = self.get_users([username], by_usernames=True, user_fields='', expansions='', tweet_fields='')['users']
        self.search_args['output_format'] = self.output_format
        if user:
            user_id = user[0]['id']
            logging.info(f"Username {username} has ID {user_id}")
        else:
            user_id = None
            logging.info(f"Username {username} does not exist")
        
        return user_id
    
    def id_to_username(self, user_id):
        logging.info(f"Translating {user_id} to username")
        username = self.get_users([user_id], by_usernames=False, user_fields='', expansions='', tweet_fields='')['users'][0]['username']
        logging.info(f"ID {user_id} has ID {username}")
        
        return username

    def search_tweets(self, search_query, user_fields=USER_FIELDS, expansions=EXPANSIONS,
                    place_fields=PLACE_FIELDS, tweet_fields=TWEET_FIELDS, other_query_args = {}, results_per_call=100, max_results=5000):

        query = gen_request_parameters(
            api="search",
            query=search_query,
            expansions=expansions,
            place_fields=place_fields,
            tweet_fields=tweet_fields,
            user_fields=user_fields,
            results_per_call=results_per_call,
            **other_query_args)
        logging.info(f"Performing search for {search_query}, returning {results_per_call} results per call. Max of {max_results} results")
        results = collect_results(query, max_results=max_results, result_stream_args=self.search_args)
        logging.debug(f"Returned {len(results)} results")
        if self.search_args['output_format'] == 'm':
            results = extract_expansions_and_tweets(results)
        return results

    def get_users_timeline_tweets(self, user_id, user_fields=USER_FIELDS, expansions=EXPANSIONS,
                    place_fields=PLACE_FIELDS, tweet_fields=TWEET_FIELDS, results_per_call=100, max_results=10000):
        query = gen_request_parameters(
            api="timeline",
            id=user_id,
            expansions=expansions,
            place_fields=place_fields,
            tweet_fields=tweet_fields,
            user_fields=user_fields,
            results_per_call=results_per_call)
        logging.info(f"Fetching timeline for user ID {user_id} returning {results_per_call} results per call")
        results = collect_results(query, max_results=max_results, result_stream_args=self.search_args)
        logging.debug(f"Returned {len(results)} results")
        if self.search_args['output_format'] == 'm':
                results = extract_expansions_and_tweets(results)
        return results

    def get_tweets_by_ids(self, tweet_ids, user_fields=USER_FIELDS, expansions=EXPANSIONS,
                    place_fields=PLACE_FIELDS,tweet_fields=TWEET_FIELDS):

        logging.info(f"Fetching {len(tweet_ids)} tweets")
        split_tweet_ids = ghetto_split(tweet_ids)
        results = []
        for split in split_tweet_ids:
            logging.info(f"Requesting {len(split)} tweets of {len(tweet_ids)} tweets. {len(results)} total tweets fetched so far.")
            query = gen_request_parameters(
                api="tweets",
                ids=split,
                expansions=expansions,
                place_fields=place_fields,
                tweet_fields=tweet_fields,
                user_fields=user_fields)
            results.extend(collect_results(query, max_results=len(tweet_ids) + 100, result_stream_args=self.search_args))

        results = collect_results(query, max_results=len(tweet_ids) + 100, result_stream_args=self.search_args)
        result_tweet_ids = [i["id"] for i in results if "text" in i.keys()]
        missing_tweet_ids = [i for i in tweet_ids if i not in result_tweet_ids]

        logging.debug(f"Returned {len(results)} tweets out of {len(tweet_ids)} requested tweets")
        logging.debug(f"{len(missing_tweet_ids)} Missing tweets: \n {missing_tweet_ids}")
        if self.search_args['output_format'] == 'm':
            results = extract_expansions_and_tweets(results)
        return results

    def get_retweeted_by(self, tweet_id, user_fields=USER_FIELDS, expansions="pinned_tweet_id",
                tweet_fields=TWEET_FIELDS):

        logging.info(f'Fetching rewteets for Tweet ID {tweet_id}')
        query = gen_request_parameters(
            api='retweeted_by',
            id=tweet_id,
            expansions=expansions,
            tweet_fields=tweet_fields,
            user_fields=user_fields)
        results = collect_results(query, result_stream_args=self.search_args)

        if self.search_args['output_format'] == 'm':
            results = extract_expansions_and_tweets(results)
        return results

    def get_followers(self, user_id, user_fields=USER_FIELDS, expansions="pinned_tweet_id",
                    tweet_fields=TWEET_FIELDS, sleep=0, results_per_call=1000, max_results=5000):
        query = gen_request_parameters(
            api="followers",
            id=user_id,
            expansions=expansions,
            tweet_fields=tweet_fields,
            user_fields=user_fields,
            results_per_call=results_per_call)
        logging.info(f"Performing follower_lookup for {user_id}, returning {results_per_call} results per call. Max of {max_results} results")
        results = collect_results(query, max_results=max_results, result_stream_args=self.search_args, sleep=sleep)
        logging.debug(f"Returned {len(results)} results")
        if self.search_args['output_format'] == 'm':
            results = extract_expansions_and_tweets(results)
        return results

    def get_following(self, user_id, user_fields=USER_FIELDS, expansions="pinned_tweet_id",
                    tweet_fields=TWEET_FIELDS, sleep=0, results_per_call=1000, max_results=5000):
        query = gen_request_parameters(
            api="following",
            id=user_id,
            expansions=expansions,
            tweet_fields=tweet_fields,
            user_fields=user_fields,
            results_per_call=results_per_call)
        logging.info(f"Performing following lookup for {user_id}, returning {results_per_call} results per call. Max of {max_results} results")
        results = collect_results(query, max_results=max_results, result_stream_args=self.search_args, sleep=sleep)
        logging.debug(f"Returned {len(results)} results")
        if self.search_args['output_format'] == 'm':
            results = extract_expansions_and_tweets(results)
        return results
    
    def get_users(self, identifiers, by_usernames=False, user_fields=USER_FIELDS, expansions="pinned_tweet_id",
                    tweet_fields=TWEET_FIELDS, results_per_call=100):
        if by_usernames:
            api = "users_by_name"
            log_api_str = "user names"
        else:
            api = "users"
            log_api_str = "user ids"

        logging.info(f"Fetching {len(identifiers)} users by {log_api_str}")
        split_identifiers = ghetto_split(identifiers)
        results = []
        for split in split_identifiers:
            logging.info(f"Requesting {len(split)} users of {len(identifiers)} users. {len(results)} total users fetched so far.")
            query = gen_request_parameters(
                api=api,
                ids=split,
                expansions=expansions,
                tweet_fields=tweet_fields,
                user_fields=user_fields)
            results.extend(collect_results(query, max_results=len(identifiers) + 100, result_stream_args=self.search_args))

        logging.debug(f"Returned {len(results)} total results")
        if self.search_args['output_format'] == 'm':
            results = extract_expansions_and_tweets(results)
        return results

    def get_users_v1(self, identifiers, by_usernames=False, tweet_mode='extended'):
        logging.info(f"Fetching {len(identifiers)} users by {'username' if by_usernames else 'user ids'}")
        split_identifiers = ghetto_split(identifiers)
        results = []
        for split in split_identifiers:
            logging.info(f"Requesting {len(split)} users of {len(identifiers)} users. {len(results)} total users fetched so far.")
            if by_usernames:
                results.extend(self.tweepy.lookup_users(screen_names = split, tweet_mode=tweet_mode))
            else:
                results.extend(self.tweepy.lookup_users(user_ids = split, tweet_mode=tweet_mode))

        logging.info(f"Returned {len(results)} users out of {len(identifiers)} requested users")
        return results
    
    def get_users_with_likes(self, identifiers, by_usernames=False):
        logging.info(f"Fetching {len(identifiers)} users with liked count")
        users_v2 = self.get_users(identifiers, by_usernames=by_usernames)
        users_v1 = self.get_users_v1(identifiers, by_usernames=by_usernames)
        if len(users_v2['users']) != len(users_v1):
            logging.warning(f"Result number mismatch - users V2 returned {len(users_v2['users'])} results and users V1 returned {len(users_v1)}")
        full_user_data = []
        for user_v1 in users_v1:
            user_v2 = [u for u in users_v2['users'] if u['id'] == user_v1.id_str][0]
            user_v2['public_metrics']['likes_count'] = user_v1.favourites_count
            full_user_data.append(user_v2)
        results = {'users': full_user_data, 'tweets': users_v2['tweets']}
        logging.info(f"Returned {len(results['users'])} user IDs out of {len(identifiers)} request IDs")
        return results

    def get_follower_ids_v1(self, screen_name=None, user_id=None, max_results=None):
        logging.info(f"Getting v1 follower ids for {screen_name if screen_name else user_id}")
        follower_list = []
        if screen_name:
            for page in tweepy.Cursor(self.tweepy.followers_ids, screen_name=screen_name, count=5000).pages():
                if max_results and len(follower_list) >= max_results:
                    follower_list.extend(page)
                    logging.info(f"Fetched {len(page)} user ids. {len(follower_list)} total IDs have been fetched. Hit max results of {max_results} - breaking.")
                    break
                else:
                    follower_list.extend(page)
                    logging.info(f"Fetched {len(page)} user ids. {len(follower_list)} total IDs have been fetched.")
        else:
            for page in tweepy.Cursor(self.tweepy.followers_ids, user_id=user_id, count=5000).pages():
                follower_list.extend(page)
                logging.info(f"Fetched {len(page)} user ids. {len(follower_list)} total IDs have been fetched.")
        return follower_list

    def get_following_ids_v1(self, screen_name=None, user_id=None, max_results=None):
        logging.info(f"Getting v1 following ids for {screen_name if screen_name else user_id}")
        friend_list = []
        if screen_name:
            for page in tweepy.Cursor(self.tweepy.friends_ids, screen_name=screen_name, count=5000).pages():
                if max_results and len(friend_list) >= max_results:
                    friend_list.extend(page)
                    logging.info(f"Fetched {len(page)} user ids. {len(friend_list)} total IDs have been fetched. Hit max results of {max_results} - breaking.")
                    break
                else:
                    friend_list.extend(page)
                    logging.info(f"Fetched {len(page)} user ids. {len(friend_list)} total IDs have been fetched.")
        else:
            for page in tweepy.Cursor(self.tweepy.friends_ids, user_id=user_id, count=5000).pages():
                friend_list.extend(page)
                logging.info(f"Fetched {len(page)} user ids. {len(friend_list)} total IDs have been fetched.")
        return friend_list
