CALL gds.graph.create('default-graph','*','*')
CALL gds.graph.drop('default-graph');

// Degree of nodes in the graph
CALL gds.alpha.degree.stream('default-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).screen_name AS name,
       score AS degree
ORDER BY degree DESC
LIMIT 50

CALL gds.graph.create.cypher(
    'user-graph',
    'MATCH (u:User) RETURN id(u) AS id',
    'MATCH (u:)-[:TWEETS]->(b:Tweet)-[:MENTIONS]->() RETURN id(a) AS source, id(b) AS target'
)

CALL gds.graph.create.cypher(
    'user-graph',
    'MATCH (u:User) RETURN id(u) AS id LIMIT 5000',
    'MATCH (u1:User)-[:POSTS]->(t1:Tweet)-[:RETWEETS]->()<-[:POSTS]-(u2:User) RETURN id(u1) AS source, id(u2) as target, count(t1) as weight UNION
     MATCH (u3:User)-[:POSTS]->(t2:Tweet)-[:MENTIONS]->(u4:User) RETURN id(u3) AS source, id(u4) as target, count(t2) as weight UNION
     MATCH (u5:User)-[:POSTS]->(t3:Tweet)-[:REPLY_TO]->()<-[:POSTS]-(u6:User) RETURN id(u5) AS source, id(u6) as target, count(t3) as weight UNION
     MATCH (u7:User)-[:POSTS]->(t4:Tweet)-[:TAGS]->()<-[:TAGS]-()<-[:POSTS]-(u8:User) RETURN id(u7) AS source, id(u8) as target, count(t4) as weight'
)
YIELD graphName, nodeCount, relationshipCount, createMillis;

CALL gds.graph.create.cypher(
    'user-graph',
    'MATCH (u:User) RETURN id(u) AS id LIMIT 1000 UNION MATCH (h:Hashtag) RETURN id(h) as id LIMIT 500',
    'MATCH (u1:User)-[:POSTS]->(t1:Tweet)-[:RETWEETS]->()<-[:POSTS]-(u2:User) RETURN id(u1) AS source, id(u2) as target, count(t1) as weight UNION
     MATCH (u3:User)-[:POSTS]->(t2:Tweet)-[:MENTIONS]->(u4:User) RETURN id(u3) AS source, id(u4) as target, count(t2) as weight UNION
     MATCH (u5:User)-[:POSTS]->(t3:Tweet)-[:REPLY_TO]->()<-[:POSTS]-(u6:User) RETURN id(u5) AS source, id(u6) as target, count(t3) as weight UNION
     MATCH (u7:User)-[t4:TAGS]->(h:Hashtag) RETURN id(u7) AS source, id(h) as target, count(t4) as weight'
)

MATCH (u1:User)-[:POSTS]->(t1:Tweet)-[:RETWEETS]->()<-[:POSTS]-(u2:User) WHERE u1.louvain = 1 AND u2.louvain = 1 RETURN * UNION
     MATCH (u3:User)-[:POSTS]->(t2:Tweet)-[:MENTIONS]->(u4:User) WHERE u2.louvain = 1 AND u3.louvain = 1 RETURN * UNION
     MATCH (u5:User)-[:POSTS]->(t3:Tweet)-[:REPLY_TO]->()<-[:POSTS]-(u6:User) WHERE u5.louvain = 1 AND u6.louvain = 1 RETURN * UNION
     MATCH (u7:User)-[t4:TAGS]->(h:Hashtag) WHERE u7.louvain = 1 RETURN *

// Visualize cluster
:params {cluster: 1}
MATCH (u1:User)-[:POSTS]->(t1:Tweet)-[:RETWEETS]->(t2:Tweet)<-[:POSTS]-(u2:User) WHERE u1.louvain = $cluster AND u2.louvain = $cluster RETURN u1 as user1, t1 as tweet1, u2 as user2, t2 as tweet2, null as tag UNION ALL
     MATCH (u1:User)-[:POSTS]->(t1:Tweet)-[:MENTIONS]->(u2:User) WHERE u1.louvain = $cluster AND u2.louvain = $cluster RETURN u1 as user1, t1 as tweet1, u2 as user2, null as tweet2, null as tag UNION ALL
     MATCH (u1:User)-[:POSTS]->(t1:Tweet)-[:REPLY_TO]->(t2:Tweet)<-[:POSTS]-(u2:User) WHERE u1.louvain = $cluster AND u2.louvain = $cluster RETURN u1 as user1, t1 as tweet1, u2 as user2, t2 as tweet2, null as tag UNION ALL
     MATCH (u1:User)-[:TAGS]->(h:Hashtag) WHERE u1.louvain = $cluster RETURN u1 as user1, null as tweet1, null as user2, null as tweet2, h as tag
// jk this better
MATCH p=(a:User)-[*..5]->(b:User)
WHERE a.louvain = 1 AND b.louvain = 1
RETURN p
//??? slow af but actually works
MATCH p=(a:User)-[*..3]->(b:User)
WHERE a.louvain = $cluster AND b.louvain = $cluster
RETURN p
UNION
MATCH p=(a:User)-[*..2]->()<-[*..2]-(b:User)
WHERE a.louvain = $cluster AND b.louvain = $cluster
RETURN p
// get bot clusters
MATCH p=(a:User)-[*..3]->(b:User)
WHERE a.louvain = b.louvain AND a.cap_english > 0.5 AND b.cap_english > 0.5
RETURN p
LIMIT 100
UNION
MATCH p=(a:User)-[*..2]->()<-[*..2]-(b:User)
WHERE a.louvain = b.louvain AND a.cap_english > 0.5 AND b.cap_english > 0.5
RETURN p
LIMIT 100


CALL gds.graph.create.cypher(
    'user-graph',
    'MATCH (u:User) RETURN id(u) AS id',
    'MATCH (u:User)-[:TWEETS]->(t:Tweet)-[:RETWEETS]->(t2:Tweet)<-[:TWEETS]-(p1:User) RETURN id(u) AS source, id(p1) as target, count(t) AS weight'
)

CALL gds.louvain.write('default-graph',
    {maxIterations:10,
     writeProperty:'louvain'})
YIELD ranLevels, communityCount,modularity,modularities