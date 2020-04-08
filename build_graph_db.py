import pandas as pd
from tqdm import tqdm
import json
import config
from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError
from os import path
import glob
import gzip
from py2neo import Graph, Node, Relationship




def add_tweet_to_graph(graph, tweet_ob):
    tweet = Node("Tweet", id=tweet_ob.id, text=tweet_ob.text, created_at=tweet_ob.created_at_datetime, lang=tweet_ob.lang)
    tweet.__primarylabel__ = "Tweet"
    tweet.__primarykey__ = "id"
    if graph.exists(tweet):
        return
    user = Node("User", screen_name=tweet_ob.screen_name, id=tweet_ob.user_id)
    user.__primarylabel__ = "User"
    user.__primarykey__ = "id"
    graph.merge(Relationship(user, "POSTS", tweet))

    for h in tweet_ob.hashtags:
        hashtag = Node("Hashtag", name=h.lower())
        hashtag.__primarylabel__ = "Hashtag"
        hashtag.__primarykey__ = "name"
        graph.merge(Relationship(tweet, "TAGS", hashtag))
    for m in tweet_ob.user_mentions:
        mention = Node("User", id=m['id_str'], screen_name=m['screen_name'])
        mention.__primarylabel__ = "User"
        mention.__primarykey__ = "id"
        graph.merge(Relationship(tweet, "MENTIONS", mention))
    reply = tweet_ob.in_reply_to_status_id
    if reply:
        reply_tweet = Node("Tweet", id=reply)
        reply_tweet.__primarylabel__ = "Tweet"
        reply_tweet.__primarykey__ = "id"
        graph.merge(Relationship(tweet, "REPLY_TO", reply_tweet))
    r = tweet_ob.retweeted_tweet
    if r:
        retweet = Node("Tweet", id=r.id)
        retweet.__primarylabel__ = "Tweet"
        retweet.__primarykey__ = "id"
        graph.merge(Relationship(tweet, "RETWEETS", retweet))

def create_tweet_graph(graph):
    print("Creating tweet graph...")
    all_tweet_files = []
    for data_folder in config.DATA_FOLDERS:
        data_path = path.join(config.DATA_DIR, data_folder, "*.jsonl.gz")
        all_tweet_files.extend(glob.glob(data_path))

    print("Starting to add tweets...")
    for tweet_file in all_tweet_files:
        print("Adding", tweet_file)
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
    create_tweet_graph(graph)

