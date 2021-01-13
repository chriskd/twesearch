from searchtweets import ResultStream, gen_request_parameters, load_credentials, collect_results
import tweepy
from decimal import Decimal
import json
import logging
import sys

EXPANSIONS = "entities.mentions.username,in_reply_to_user_id,author_id,geo.place_id,\
            referenced_tweets.id.author_id,referenced_tweets.id"
USER_FIELDS = "created_at,description,entities,id,location,name,pinned_tweet_id,\
            profile_image_url,protected,public_metrics,url,username,verified,withheld"
PLACE_FIELDS = "contained_within,country,country_code,full_name,geo,id,name,place_type"
TWEET_FIELDS = "author_id,text,context_annotations,conversation_id,created_at,entities,geo,\
            in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,withheld"


class Twesearch:

    def __init__(self,log=False, log_level="info", defloat=False):
        if log:
            fmt="%(levelname)s:%(module)s:%(funcName)s():%(lineno)i: %(message)s"
            if log_level == "debug":
                logging.basicConfig(stream=sys.stdout,level=logging.DEBUG,format=fmt)
            elif log_level == "info":
                logging.basicConfig(stream=sys.stdout,level=logging.INFO)
            logger = logging.getLogger()
            formatter = logging.Formatter(fmt)
            logger.handlers[0].setFormatter(formatter)

        self.search_args = load_credentials("~/.twitter_keys.yaml",
                                       yaml_key="search_tweets_v2",
                                       env_overwrite=False)
        self.tweepy = tweepy.API(tweepy.AppAuthHandler(bearer_token=self.search_args['bearer_token']), wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.defloat = defloat

    def _ghetto_split(self, list_, chunk_size=100):
        """
        Utility function to split a list into a list of lists of size chunk_size
        """
        logging.debug(f"Splitting list of {len(list_)} length, chunk size = {chunk_size}")
        split_lists = []
        for i in range(0,len(list_),chunk_size):
            split_lists.append(list_[i:i+chunk_size])
        logging.debug(f"List has been split into {len(split_lists)} lists. Total num of elements in split lists is {sum([len(i) for i in split_lists])}")
        return split_lists

    def _extract_expansions_and_tweets(self, results):
        logging.info("Separating tweets, users and places")

        tweets = [i for i in results if 'text' in i.keys()]
        logging.info(f"Fetched {len(tweets)} tweets from outer list")
        expanded_tweets = [t for i in [e['tweets'] for e in results if 'tweets' in e.keys()] for t in i if 'text' in t.keys() ]
        logging.info(f"Fetched {len(expanded_tweets)} from Tweet expansions object")

        users = [i for i in results if 'username' in i.keys()]
        logging.info(f"Fetched {len(users)} users from outer list")
        expanded_users = [t for i in [e['users'] for e in results if 'users' in e.keys()] for t in i]
        logging.info(f"Fetched {len(expanded_users)} from Users expansion object")

        extracted_resuts = {"og_tweet_count": len(tweets), "og_users_count": len(users),
                            "expanded_tweet_count": len(expanded_tweets), "expanded_users_count": len(expanded_users)}

        tweets = tweets + expanded_tweets
        users = users + expanded_users
        if self.defloat:
            tweets = self._defloat(tweets)
            users = self._defloat(users)
        logging.info(f"Final stats: {len(tweets)} total tweets, {len(users)} total users")
        return {'tweets': tweets, 'users': users}
    
    def _defloat(self, results):
        return json.loads(json.dumps(results), parse_float=Decimal)

    def return_search_args(self):
        return self.search_args

    def return_tweepy_api(self):
        return self.tweepy

    def search_tweets(self, search_query, user_fields=USER_FIELDS, expansions=EXPANSIONS,
                    place_fields=PLACE_FIELDS, tweet_fields=TWEET_FIELDS, results_per_call=100, max_results=5000):
        query = gen_request_parameters(
            api="search",
            query=search_query,
            expansions=expansions,
            place_fields=place_fields,
            tweet_fields=tweet_fields,
            user_fields=user_fields,
            results_per_call=results_per_call)
        logging.info(f"Performing search for {search_query}, returning {results_per_call} results per call. Max of {max_results} results")
        results = collect_results(query, max_results=max_results, result_stream_args=self.search_args)
        logging.info(f"Returned {len(results)} results")
        results = self._extract_expansions_and_tweets(results)
        return results

    def get_tweets(self, tweet_ids, user_fields=USER_FIELDS, expansions=EXPANSIONS,
                    place_fields=PLACE_FIELDS,tweet_fields=TWEET_FIELDS):

        logging.info(f"Fetching {len(tweet_ids)} tweets")
        if len(tweet_ids) > 100:
            logging.info(f"Over 100 tweets requested, chunking requests per 100")
            split_tweet_ids = self._ghetto_split(tweet_ids)
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
        else:
            query = gen_request_parameters(
                api="tweets",
                ids=tweet_ids,
                expansions=expansions,
                tweet_fields=tweet_fields,
                user_fields=user_fields)
            results = collect_results(query, max_results=len(tweet_ids) + 100, result_stream_args=self.search_args)
        result_tweet_ids = [i["id"] for i in results if "text" in i.keys()]
        missing_tweet_ids = [i for i in tweet_ids if i not in result_tweet_ids]

        logging.info(f"Returned {len(results)} tweets out of {len(tweet_ids)} requested tweets")
        logging.debug(f"{len(missing_tweet_ids)} Missing tweets: \n {missing_tweet_ids}")
        results = self._extract_expansions_and_tweets(results)
        return results

    def get_followers(self, user_id, user_fields=USER_FIELDS, expansions="pinned_tweet_id",
                    tweet_fields=TWEET_FIELDS, results_per_call=1000, max_results=5000):
        query = gen_request_parameters(
            api="followers",
            id=user_id,
            expansions=expansions,
            tweet_fields=tweet_fields,
            user_fields=user_fields,
            results_per_call=results_per_call)
        logging.info(f"Performing follower_lookup for {user_id}, returning {results_per_call} results per call. Max of {max_results} results")
        results = collect_results(query, max_results=max_results, result_stream_args=self.search_args)
        logging.info(f"Returned {len(results)} results")
        results = self._extract_expansions_and_tweets(results)
        return results

    def get_following(self, user_id, user_fields=USER_FIELDS, expansions="pinned_tweet_id",
                    tweet_fields=TWEET_FIELDS, results_per_call=1000, max_results=5000):
        query = gen_request_parameters(
            api="following",
            id=user_id,
            expansions=expansions,
            tweet_fields=tweet_fields,
            user_fields=user_fields,
            results_per_call=results_per_call)
        logging.info(f"Performing following lookup for {user_id}, returning {results_per_call} results per call. Max of {max_results} results")
        results = collect_results(query, max_results=max_results, result_stream_args=self.search_args)
        logging.info(f"Returned {len(results)} results")
        results = self._extract_expansions_and_tweets(results)
        return results

    def get_users(self, identifiers, by_usernames=False, user_fields=USER_FIELDS, expansions="pinned_tweet_id",
                    tweet_fields=TWEET_FIELDS, results_per_call=100):
        if by_usernames:
            api = "users_by_name"
        else:
            api = "users"

        logging.info(f"Fetching {len(identifiers)} users by {api}")
        if len(identifiers) > 100:
            logging.info(f"Over 100 users requested, chunking requests per 100")
            split_identifiers = self._ghetto_split(identifiers)
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
        else:
            query = gen_request_parameters(
                api=api,
                ids=identifiers,
                expansions=expansions,
                tweet_fields=tweet_fields,
                user_fields=user_fields)
            print(len(identifiers))
            results = collect_results(query, max_results=len(identifiers) + 100, result_stream_args=self.search_args)

        logging.info(f"Returned {len(results)} results")
        results = self._extract_expansions_and_tweets(results)
        return results

    def get_users_v1(self, identifiers, by_usernames=False, tweet_mode='extended'):
        logging.info(f"Fetching {len(identifiers)} users by {'username' if by_usernames else 'user ids'}")
        if len(identifiers) > 100:
            logging.info(f"Over 100 users requested, chunking requests per 100")
            split_identifiers = self._ghetto_split(identifiers)
            results = []
            for split in split_identifiers:
                logging.info(f"Requesting {len(split)} users of {len(identifiers)} users. {len(results)} total users fetched so far.")
                if by_usernames:
                    results.extend(self.tweepy.lookup_users(screen_names = split, tweet_mode=tweet_mode))
                else:
                    results.extend(self.tweepy.lookup_users(user_ids = split, tweet_mode=tweet_mode))
        else:
            if by_usernames:
                results = self.tweepy.lookup_users(screen_names = identifiers, tweet_mode=tweet_mode)
            else:
                results = self.tweepy.lookup_users(user_ids = identifiers, tweet_mode=tweet_mode)

        logging.info(f"Returned {len(results)} users out of {len(identifiers)} requested users")
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
