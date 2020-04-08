import botometer
import json
from tweepy.error import TweepError
from py2neo import Graph, Node, Relationship, cypher_escape

def get_bot_info(graph):
    with open("twitter_auth.json") as f:
        twitter_app_auth = json.load(f)
    bom = botometer.Botometer(wait_on_ratelimit=True,
                              **twitter_app_auth)

    # All users who tweeted more than once
    for user in graph.run("""MATCH (n:User) WHERE n.cap_english IS NULL 
                WITH n MATCH p=(n)-[r:POSTS]->()
                WITH count(r) as cntr, n
                WHERE cntr>1
                RETURN n"""):
        try:
            api_result = bom.check_account(user['screen_name'])
        except TweepError as e:
            print(e)
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
                tx.run("\n".join(clauses), parameters)
        for relationship in subgraph.relationships:
            if relationship.graph is graph:
                clauses = ["MATCH ()-[_]->() WHERE id(_) = $x", "SET _ = $y"]
                parameters = {"x": relationship.identity, "y": dict(relationship)}
                tx.run("\n".join(clauses), parameters)


if __name__ == "__main__":
    graph = Graph()
    get_bot_info(graph)