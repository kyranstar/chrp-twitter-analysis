import pandas as pd
from tqdm import tqdm
import json
import config
from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError
from os import path
import glob
import gzip
from py2neo import Graph




def add_tweet_to_graph(graph, tweet_ob):
    # Create tweet object if it doesn't exist, create user object if doesn't exist, create user -POSTS-> tweet
    graph.run("""MERGE (t:Tweet{id:$tweet_id})
    SET t.text = $tweet_text
    SET t.created_at = $tweet_created_at
    SET t.lang = $tweet_lang
    MERGE (u:User{id:$user_id})
    SET u.screen_name = $user_screen_name
    MERGE (u)-[r:POSTS]->(t)
    """, {"tweet_id": tweet_ob.id,
          "tweet_text": tweet_ob.text,
          "tweet_created_at": tweet_ob.created_at_datetime,
          "tweet_lang": tweet_ob.lang,
          "user_id": tweet_ob.user_id,
          "user_screen_name": tweet_ob.screen_name})

    # Create hashtag if doesn't exist, create tweet -TAGS-> hashtag
    for h in tweet_ob.hashtags:
        graph.run("""MERGE (t:Tweet{id:$tweet_id})
                   MERGE (h:Hashtag{name:$h_name})
                   MERGE (t)-[r:TAGS]->(h)
                   """, {"tweet_id": tweet_ob.id,
                         'h_name': h.lower()})
    # Create other user if doesn't exist, create tweet -TAGS-> hashtag
    for m in tweet_ob.user_mentions:
        graph.run("""MERGE (t:Tweet{id:$tweet_id})
           MERGE (u:User{id:$mentioned_user_id})
           SET u.screen_name = $mentioned_user_screen_name
           MERGE (t)-[r:MENTIONS]->(u)
           """, {"tweet_id": tweet_ob.id,
                 "mentioned_user_id": m['id_str'],
                 "mentioned_user_screen_name": m['screen_name']})
    if tweet_ob.in_reply_to_status_id:
        graph.run("""MERGE (t:Tweet{id:$tweet_id})
                   MERGE (r:Tweet{id:$reply_id})
                   SET r.screen_name = $r_screen_name
                   MERGE (t)-[q:REPLY_TO]->(r)
                   """, {"tweet_id": tweet_ob.id,
                         "reply_id": tweet_ob.in_reply_to_status_id,
                         "r_screen_name": tweet_ob.in_reply_to_screen_name})
    r = tweet_ob.retweeted_tweet
    if r:
        graph.run("""MERGE (t:Tweet{id:$tweet_id})
                    MERGE (h:Tweet{id:$rt_id})
                    SET h.text = $rt_text
                    SET h.created_at = $rt_created_at
                    SET h.lang = $rt_lang
                    MERGE (t)-[r:RETWEETS]->(h)
                   """, {"tweet_id": tweet_ob.id,
                         'rt_id': r.id,
                         'rt_text': r.text,
                         'rt_created_at': r.created_at_datetime,
                         'rt_lang':r.lang})

def create_tweet_graph(graph, start_at=None):
    print("Creating tweet graph...")
    all_tweet_files = []
    for data_folder in config.DATA_FOLDERS:
        data_path = path.join(config.DATA_DIR, data_folder, "*.jsonl.gz")
        all_tweet_files.extend(glob.glob(data_path))

    if start_at is not None:
        all_tweet_files = all_tweet_files[all_tweet_files.index(start_at):]

    print("Starting to add tweets...")
    for tweet_file in all_tweet_files:
        print("Adding", tweet_file)
        try:
            with gzip.open(tweet_file, 'rb') as f:
                for tweet_dict in tqdm(f):
                    try:
                        tweet = Tweet(json.loads(tweet_dict))
                        add_tweet_to_graph(graph, tweet)
                    except (json.JSONDecodeError, NotATweetError) as e:
                        print(e)
                        print(tweet)
                        pass
                    except Exception as e:
                        print(tweet)
                        raise e
        except EOFError as e:
            print(e)
            continue


def constraint(label, property):
    graph = Graph()
    schema = graph.schema
    if property not in schema.get_uniqueness_constraints(label):
        schema.create_uniqueness_constraint(label, property)



if __name__ == "__main__":
    graph = Graph()
    #constraint("Tweet", "id")
    #constraint("User", "screen_name")
    #constraint("Hashtag", "name")
    create_tweet_graph(graph, start_at=path.join(config.DATA_DIR, "2020-01", "coronavirus-tweet-id-2020-01-26-14.jsonl.gz"))

