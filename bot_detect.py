import botometer
import pandas as pd
import numpy as np
from tqdm import tqdm
import json
from os import path


import config


data_dir = ""

def get_bot_info(user_df):
    """

    :param user_df:
    :return:
    """
    with open("twitter_auth.json") as f:
        twitter_app_auth = json.load(f)
    bom = botometer.Botometer(wait_on_ratelimit=True,
                              **twitter_app_auth)

    # cat_ are judgements based on certain categories
    # bot_score is judgement based on all categories
    # cap is the complete automation score, using bayes theorem on bot_score to get probability it is a bot
    for result_col in ['cap_english', 'cap_universal', 'cat_content', 'cat_friend', 'cat_network', 'cat_sentiment', 'cat_temporal', 'cat_user', 'bot_score_english', 'bot_score_universal']:
        if result_col not in user_df.columns:
            user_df[result_col] = np.nan
    user_df = user_df.head(10)

    for i, row in tqdm(user_df.iterrows()):
        if not np.isnan(row['cap_english']):
            continue
        api_result = bom.check_account(row['screen_name'])
        user_df.at[i, 'cap_english'] = api_result['cap']['english']
        user_df.at[i, 'cap_universal'] = api_result['cap']['universal']
        user_df.at[i, 'cat_content'] = api_result['categories']['content']
        user_df.at[i, 'cat_friend'] = api_result['categories']['friend']
        user_df.at[i, 'cat_network'] = api_result['categories']['network']
        user_df.at[i, 'cat_sentimental'] = api_result['categories']['sentiment']
        user_df.at[i, 'cat_temporal'] = api_result['categories']['temporal']
        user_df.at[i, 'cat_user'] = api_result['categories']['user']
        user_df.at[i, 'bot_score_english'] = api_result['scores']['english']
        user_df.at[i, 'bot_score_universal'] = api_result['scores']['universal']
    return user_df


if __name__ == "__main__":
    if path.exists(config.USER_DF_PATH):
        user_df = pd.read_pickle(config.USER_DF_PATH)
        user_df = get_bot_info(user_df)
        pd.to_pickle(user_df, config.USER_DF_PATH)
    else:
        print("User data frame does not exist", config.USER_DF_PATH)
