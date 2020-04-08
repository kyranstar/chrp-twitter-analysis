import pandas as pd
from tqdm import tqdm
import json
import config
from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError
from os import path
import glob
import gzip


def create_tweet_df():
    """
    Creates a pandas dataframe representing all of the coronavirus tweets stored in DATA_DIR/f/*.jsonl.gz where f is in
    DATA_FOLDERS.
    :return: the tweet dataframe
    """

    tweet_df = pd.DataFrame(
            columns=['tweet_id', 'text', 'screen_name', 'user_id', 'name', 'bio', 'creation_date', 'profile_location',
                     'tweet_type', 'embedded_tweet', 'lang', 'hashtags', 'user_mentions', 'tweet_links', 'generator'])\
        .astype({'tweet_id': 'str', 'user_id': 'int32'})
    all_tweet_files = []
    for data_folder in config.DATA_FOLDERS:
        data_path = path.join(config.DATA_DIR, data_folder, "*.jsonl.gz")
        all_tweet_files.extend(glob.glob(data_path))

    for tweet_file in all_tweet_files:
        with gzip.open(tweet_file, 'rb') as f:
            for tweet_dict in tqdm(f):
                try:
                    tweet = Tweet(json.loads(tweet_dict))
                    tweet_df = tweet_df.append({'tweet_id': tweet.id,
                                                'text': tweet.all_text,
                                                'screen_name': tweet.screen_name,
                                                'user_id': tweet.user_id,
                                                'name': tweet.name,
                                                'bio': tweet.bio,
                                                'creation_date': tweet.created_at_datetime,
                                                'profile_location': tweet.profile_location,
                                                'tweet_type': tweet.tweet_type,
                                                'embedded_tweet': tweet.embedded_tweet,
                                                'lang': tweet.lang,
                                                'hashtags': tweet.hashtags,
                                                'user_mentions': tweet.user_mentions,
                                                'tweet_links': tweet.tweet_links,
                                                'generator': tweet.generator, }, ignore_index=True)
                except (json.JSONDecodeError, NotATweetError) as e:
                    print(e)
                    print(tweet)
                    pass
                except Exception as e:
                    print(tweet)
                    raise e
    return tweet_df


if __name__ == "__main__":
    tweet_df = create_tweet_df()
    print(tweet_df)
    pd.to_pickle(tweet_df, config.TWEET_DF_PATH)
