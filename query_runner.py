import os
from neo4j import GraphDatabase


class Query_Runner ():

    def __init__(self, *args, **kwargs):
        self.driver = GraphDatabase.driver(
            os.getenv('NEO4J_URL'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD')))
        return super().__init__(*args, **kwargs)

    def get_publication_communities(self, keywords):
        with self.driver.session() as session:
            return session.run(f"""
                MATCH (x)-[:HAS]->(p:Paper)-[:HAS]->(k:Keyword)
                WHERE k.keyword IN {keywords}
                    WITH x, toFloat(COUNT(p)) AS totalAboutTopic
                    MATCH (x)-[:HAS]->(p1:Paper)
                        WITH x, totalAboutTopic, toFloat(COUNT(p1)) AS total, (totalAboutTopic / toFloat(COUNT(p1))) AS ratio
                        WHERE ratio > 0.9
                        RETURN COLLECT(x.title)
            """).single()[0]

    def get_top_papers(self, publications):
        with self.driver.session() as session:
            config = "{ graph: 'cypher' }"
            return session.run(f"""
                CALL algo.pageRank.stream(
                    'MATCH (p:Paper) WHERE EXISTS ((p)-[:CITED_BY]->()) RETURN id(p) AS id',
                    "MATCH (p1:Paper)-[:CITED_BY]-(p2:Paper) WHERE (p1 IN {publications}) AND (p2 IN {publications})
                     RETURN id(p1) AS source, id(p2) AS target",
    	            {config})
                YIELD nodeId, score WITH nodeId, score
                MATCH (x:Paper) WHERE id(x) = nodeId
                ORDER BY score DESC LIMIT 100
                RETURN COLLECT(node .title)
            """).single()[0]

    def get_authors(self, papers):
        with self.driver.session() as session:
            return session.run(f"""
                MATCH (a:Author)-[:WRITE]->(p:Paper)
                WHERE p.title IN {papers}
                RETURN COLLECT(DISTINCT a.name)
            """).single()[0]

    def get_gurus(self, papers, threshold):
        with self.driver.session() as session:
            return session.run(f"""
                MATCH (a:Author)-[:WRITE]->(p:Paper)
                WHERE p.title in {papers}
                WITH a.name AS name, COUNT(a.name) AS paperCount
                    WHERE paperCount >= {threshold}
                    RETURN COLLECT(name)
            """).single()[0]
