from neo4j import GraphDatabase
from .util import ghetto_split, create_stdout_logger
import logging


TWEET_IMPORT_QUERY = '''
UNWIND $tweets AS t
WITH t
ORDER BY t.tweet_id

WITH t,
     t.tweet_entities as e,
     t.tweet_referenced_tweets as refs,
     t.tweet_author_followers as followers,
     t.tweet_author_following as following

//Create Tweet Node
MERGE (tweet:Tweet {id:t.tweet_id})
SET tweet.text = t.tweet_text,
    tweet.created_at = datetime(t.tweet_created_at),
    tweet.like_count = t.tweet_public_metrics.like_count,
    tweet.retweet_count = t.tweet_public_metrics.retweet_count,
    tweet.reply_count = t.tweet_public_metrics.reply_count,
    tweet.quote_count = t.tweet_public_metrics.quote_count,
    tweet.lang = t.tweet_lang,
    tweet.source = t.tweet_source,
    tweet.author_id = t.tweet_author_id,
    tweet.author_username = t.tweet_author_username,
    tweet.camp_id = t.tweet_camp_id,
    tweet.fetched_timestamp = t.tweet_fetched_timestamp,
    tweet.author_fetched_timestamp = t.tweet_author_fetched_timestamp
SET (
CASE
WHEN tweet.retweet_count > 0
THEN tweet END).retweeted = True

//Create Source Node, and Tweet-POSTED_VIA->Source relationship
MERGE (source:Source {name:t.tweet_source})
MERGE (tweet)-[:POSTED_VIA]->(source)

//Create User node, and User-POSTED->Tweet relationship
MERGE (user:User {id:t.tweet_author_id})
SET user.name = t.tweet_author_name,
    user.username = t.tweet_author_username,
    user.location = t.tweet_author_location,
    user.followers_count = t.tweet_author_public_metrics.followers_count,
    user.following_count = t.tweet_author_public_metrics.following_count,
    user.tweets_count = t.tweet_author_public_metrics.tweet_count,
    user.listed_count = t.tweet_author_public_metrics.listed_count,
    user.profile_image_url = t.tweet_author_profile_image_url,
    user.description = t.tweet_author_description,
    user.created_at = datetime(t.tweet_author_created_at),
    user.url = toLower(t.tweet_author_url),
    user.camp_id = t.tweet_camp_id,
    user.fetched_timestamp = t.tweet_author_fetched_timestamp
MERGE (user)-[:POSTED]->(tweet)

//Iterate through follower IDs, create User node, 
//then create Follower-FOLLOWS->User relationships
FOREACH (follower in followers |
  MERGE (flwr:User {id:follower})
  MERGE (flwr)-[:FOLLOWS]->(user)
)
//Iterate through following IDs, create User node,
//then create User-FOLLOWS->Following relationship
FOREACH (following in following |
    MERGE (flwing:User {id:following})
    MERGE (user)-[:FOLLOWING]->(flwing)
)

//This section deals with unpacking the Entities enum from Twitter
//Create hashtag nodes and Tweet-HAS_TAG->Tag relationships
FOREACH (h IN e.hashtags |
  MERGE (tag:Hashtag {tag:toLower(h.tag)})
  MERGE (tweet)-[:HAS_TAG]->(tag)
)

//Create mentioned Users nodes and Tweet-TWEET_MENTIONED->User
//and User-USER_MENTIONED->User relationship
FOREACH (m IN e.mentions |
  MERGE (mentioned:User {username:m.username})
  MERGE (tweet)-[:MENTIONED]->(mentioned)
)

FOREACH (r IN [r IN refs WHERE r.type = 'replied_to'] |
  MERGE (reply_tweet:Tweet {id:r.id})
  MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)
)
//Create Tweet node, and Tweet-QUOTED_TWEET->Tweet relationship
FOREACH (q IN [q IN refs WHERE q.type = 'quoted'] |
  MERGE (quoted_tweet:Tweet {id:q.id})
  MERGE (tweet)-[:QUOTED]->(quoted_tweet)
)
//Create Tweet node, and Tweet->RETWEETED_TWEET->Tweet relationship
FOREACH (rt IN [rt IN refs WHERE rt.type = 'retweeted'] |
  MERGE (rt_tweet:Tweet {id:rt.id})
  MERGE (tweet)-[:RETWEETED]->(rt_tweet)
)
//Create URL nodes and Tweet-HAS_LINK->Link relationships
WITH e.urls as urls
UNWIND urls as url
WITH toLower(url.expanded_url) as expanded_url
WITH apoc.text.replace(expanded_url, '/$', '') as normalized_url, expanded_url
WITH apoc.text.replace(normalized_url, '^https?://', '') as normalized_url, expanded_url
WITH apoc.text.replace(apoc.text.replace(normalized_url, '(&?)(utm_.*?(?=&|$))', ''), '(\?$|(?<=\?)&)', '') as normalized_url, expanded_url
WITH collect({expanded_url: expanded_url, normalized_url: normalized_url}) as urls
FOREACH (u IN urls |
  MERGE (url:URL {url:u['normalized_url']})
  SET url.original_url = u['expanded_url']
  SET url.domain = apoc.data.domain(u['expanded_url'])
  MERGE (tweet)-[:HAS_LINK]->(url)
)
'''

USER_IMPORT_QUERY = '''
UNWIND $users as u
WITH u
ORDER BY u.id

WITH u,
     u.followers as followers,
     u.following as following

//Create User node, and User-POSTED->Tweet relationship
MERGE (user:User {id:u.id})
SET user.name = u.name,
    user.username = u.username,
    user.location = u.location,
    user.followers_count = u.public_metrics.followers_count,
    user.following_count = u.public_metrics.following_count,
    user.tweets_count = u.public_metrics.tweet_count,
    user.listed_count = u.public_metrics.listed_count,
    user.profile_image_url = u.profile_image_url,
    user.description = u.description,
    user.created_at = u.created_at,
    user.url = u.url,
    user.camp_id = u.camp_id
MERGE (user)-[:POSTED]->(tweet)

MERGE (url:URL {url:u.url})
MERGE (user)-[:HAS_LINK]->(url)

//Iterate through follower IDs, create User node, 
//then create Follower-FOLLOWS->User relationships
FOREACH (follower in followers |
  MERGE (flwr:User {id:follower})
  MERGE (flwr)-[:FOLLOWS]->(user)
)
//Iterate through following IDs, create User node,
//then create User-FOLLOWS->Following relationship
FOREACH (following in following |
    MERGE (flwing:User {id:following})
    MERGE (user)-[:FOLLOWING]->(flwing)
)
'''

class Neo4jImporter:

    def __init__(self, neo4j_uri, db_name, auth, log=False):
        driver = GraphDatabase.driver(neo4j_uri, auth=(auth['user'], 
                                        auth['password'])) 
        self.session = driver.session(database=db_name)
        self.logger = create_stdout_logger('debug')

    def insert(self, item_type, items, chunk_size=5000):
        if item_type == 'users':
            import_query = USER_IMPORT_QUERY
        elif item_type == 'tweets':
            import_query = TWEET_IMPORT_QUERY

        chunks = ghetto_split(items, chunk_size)
        inc = 0
        for chunk in chunks:
            logging.info(f"Inserting {item_type} chunk {inc} of {len(chunks)}")
            self.session.run(import_query,{item_type:chunk})
            inc += 1
