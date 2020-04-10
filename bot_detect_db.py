#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Botometer bot detection to neo4j

This script infers information about twitter users in a neo4j database by calling
the Botometer API.

This script requires that a neo4j graph database be running where there is a node type
User. This script will take all users which do not have the property cap_english
(complete automation probability) and have posted a certain number of tweets (4),
and will query the API on these users. It will then write the resulting properties
to the node in the graph.
"""
import json
from requests.exceptions import HTTPError

import botometer
from tweepy.error import TweepError
from py2neo import Graph, cypher_escape
from tqdm import tqdm

def get_bot_info(graph):
    with open("twitter_auth.json") as f:
        twitter_app_auth = json.load(f)
    bom = botometer.Botometer(wait_on_ratelimit=True,
                              **twitter_app_auth)

    # All users who tweeted more than once
    for user in tqdm(graph.run("""MATCH (n:User)
                            WHERE n.cap_english IS NULL AND size((n)-[:POSTS]->()) > 4
                            RETURN n""")):
        user = user['n']
        try:
            api_result = bom.check_account(user['screen_name'])
        except (TweepError, HTTPError) as e:
            raise(e)
            continue
        # cat_ are judgements based on certain categories
        # bot_score is judgement based on all categories
        # cap is the complete automation score, using bayes theorem on bot_score to get probability it is a bot
        user['cap_english'] = api_result['cap']['english']
        user['cap_universal'] = api_result['cap']['universal']
        user['cat_content'] = api_result['categories']['content']
        user['cat_friend'] = api_result['categories']['friend']
        user['cat_network'] = api_result['categories']['network']
        user['cat_sentimental'] = api_result['categories']['sentiment']
        user['cat_temporal'] = api_result['categories']['temporal']
        user['cat_user'] = api_result['categories']['user']
        user['bot_score_english'] = api_result['scores']['english']
        user['bot_score_universal'] = api_result['scores']['universal']
        print(f"{user['screen_name']} bot likelihood: {user['cap_english']}")
        push_subgraph(graph, user)



def push_subgraph(graph, subgraph):
    with graph.begin() as tx:
        graph = tx.graph
        for node in subgraph.nodes:
            if node.graph is graph:
                clauses = ["MATCH (_) WHERE id(_) = $x", "SET _ = $y"]
                parameters = {"x": node.identity, "y": dict(node)}
                old_labels = node._remote_labels - node._labels
                if old_labels:
                    clauses.append("REMOVE _:%s" % ":".join(map(cypher_escape, old_labels)))
                new_labels = node._labels - node._remote_labels
                if new_labels:
                    clauses.append("SET _:%s" % ":".join(map(cypher_escape, new_labels)))
                clauses.append("RETURN (_)")
                k = tx.run("\n".join(clauses), parameters)
                print(k.next())


if __name__ == "__main__":
    graph = Graph()
    get_bot_info(graph)