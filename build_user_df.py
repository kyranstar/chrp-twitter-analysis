import config
from os import path
import pandas as pd


def create_user_df(tweet_df):
    user_df = tweet_df.groupby(['user_id', 'screen_name']).agg({
        'bio': lambda tdf: tdf.unique().tolist(),
        'lang': lambda tdf: tdf.unique().tolist(),
        'name': lambda tdf: tdf.unique().tolist(),
        'profile_location': lambda tdf: tdf.unique().tolist()}).reset_index()
    return user_df


if __name__ == "__main__":
    if path.exists(config.TWEET_DF_PATH):
        tweet_df = pd.read_pickle(config.TWEET_DF_PATH)
        user_df = create_user_df(tweet_df)
        pd.to_pickle(user_df, config.USER_DF_PATH)
    else:
        print("Tweet data frame does not exist", config.TWEET_DF_PATH)
