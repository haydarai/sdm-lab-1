import os
import pandas as pd
from neo4j import GraphDatabase


class Neo4J_Loader():

    def __init__(self, *args, **kwargs):
        self.driver = GraphDatabase.driver(
            os.getenv('NEO4J_URL'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD')))
        return super().__init__(*args, **kwargs)

    def load_conferences(self):
        print('Loading conferences to Neo4J...')
        with self.driver.session() as session:
            session.run("""
                MATCH (c:Conference) DETACH DELETE c
            """)
            session.run("""
                LOAD CSV FROM 'file:///proceedings.csv' AS row
                WITH row
                    WITH toString(toInteger(row[1])) + '-01-01' AS startDate, row
                        WITH toString(toInteger(row[1])) + '-01-02' AS endDate, startDate, row
                            CREATE (c:Conference { title: row[0], startDate: startDate, endDate: endDate, edition: row[1] })
                            RETURN c
            """)
            print('Conferences loaded.')

    def add_index_to_conferences(self):
        with self.driver.session() as session:
            session.run('CREATE INDEX ON :Conference(title)')
            session.run('CREATE INDEX ON :Conference(startDate)')

    def load_journals(self):
        print('Loading journals to Neo4J...')
        with self.driver.session() as session:
            session.run("""
                MATCH (j:Journal) DETACH DELETE j
            """)
            session.run("""
                LOAD CSV FROM 'file:///journals.csv' AS row
                WITH row
                    WITH toString(toInteger(row[1])) + '-01-01' AS date, row
                        CREATE (j:Journal { title: row[0], date: date, volume: row[2] })
                        RETURN j
            """)
            print('Journals loaded.')

    def add_index_to_journals(self):
        with self.driver.session() as session:
            session.run('CREATE INDEX ON :Journal(title)')
            session.run('CREATE INDEX ON :Journal(date)')
            session.run('CREATE INDEX ON :Journal(volume)')

    def add_index_to_authors(self):
        with self.driver.session() as session:
            session.run('CREATE INDEX ON :Author(name)')

    def delete_papers(self):
        with self.driver.session() as session:
            session.run("""
                MATCH (p:Paper) DETACH DELETE p
            """)

    def load_conference_papers(self):
        print('Loading conference papers to Neo4J...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///conference_papers.csv' AS row
                WITH row
                    CREATE (p:Paper { key: row[0], title: row[1], abstract: row[4] })
                    WITH row, p
                        MATCH (c:Conference { title: row[2], startDate: toString(toInteger(row[3])) + '-01-01' })
                        CREATE (c)-[:HAS]->(p)
                        RETURN p
            """)
            print('Conference papers loaded.')

    def load_conference_paper_keywords(self):
        print('Loading conference paper keywords to Neo4J...')
        with self.driver.session() as session:
            session.run("""CREATE INDEX ON :Keyword(keyword) """)
            session.run("""
                LOAD CSV FROM 'file:///conference_paper_keywords.csv' AS row
                WITH row
                    MATCH (p:Paper { key: row[0] })
                    WITH row, p
                        MERGE (k:Keyword { keyword: row[1] })
                        CREATE (p)-[:HAS]->(k)
                        RETURN p
            """)
            print('Conference paper keywords loaded.')

    def load_journal_papers(self):
        print('Loading journal papers to Neo4J...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///journal_papers.csv' AS row
                WITH row
                    CREATE (p:Paper { key: row[0], title: row[1], abstract: row[5] })
                    WITH row, p
                        MATCH (j:Journal { title: row[2], date: toString(toInteger(row[3])) + '-01-01', volume: row[4] })
                        CREATE (j)-[:HAS]->(p)
                        RETURN p
            """)
            print('Journal papers loaded.')

    def load_journal_paper_keywords(self):
        print('Loading journal paper keywords to Neo4J...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///journal_paper_keywords.csv' AS row
                WITH row
                    MATCH (p:Paper { key: row[0] })
                    WITH row, p
                        MERGE (k:Keyword { keyword: row[1] })
                        CREATE (p)-[:HAS]->(k)
                        RETURN p
            """)
            print('Journal paper keywords loaded.')

    def add_index_to_papers(self):
        with self.driver.session() as session:
            session.run('CREATE INDEX ON :Paper(key)')

    def delete_authors(self):
        with self.driver.session() as session:
            session.run("""
                MATCH (a:Author) DETACH DELETE a
            """)

    def load_conference_venues(self):
        print('Loading conference venues...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///conference_venues.csv' AS row
                WITH row
                    MATCH (c:Conference { title: row[0] })
                    SET c.venue = row[1]
                    RETURN c
            """)
        print('Conference venues loaded.')

    def load_corresponding_conference_authors(self):
        print('Loading corresponding authors from conference papers...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///corresponding_conference_authors.csv' AS row
                WITH row
                    MERGE (a:Author { name: row[1] })
                    WITH row, a
                        MATCH (p:Paper { key: row[0] })
                        CREATE (a)-[:WRITE { is_corresponding: true }]->(p)
                        RETURN a
            """)
            print('Corresponding conference authors loaded.')

    def load_corresponding_journal_authors(self):
        print('Loading corresponding authors from journal papers...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///corresponding_journal_authors.csv' AS row
                WITH row
                    MERGE (a:Author { name: row[1] })
                    WITH row, a
                        MATCH (p:Paper { key: row[0] })
                        CREATE (a)-[:WRITE { is_corresponding: true }]->(p)
                        RETURN a
            """)
            print('Corresponding journal authors loaded.')

    def load_non_corresponding_conference_authors(self):
        print('Loading non-corresponding authors from conference papers...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///non_corresponding_conference_authors.csv' AS row
                WITH row
                    MERGE (a:Author { name: row[1] })
                    WITH row, a
                        MATCH (p:Paper { key: row[0] })
                        CREATE (a)-[:WRITE]->(p)
                        RETURN a
            """)
            print('Non-corresponding conference authors loaded.')

    def load_non_corresponding_journal_authors(self):
        print('Loading non-corresponding authors from journal papers...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///non_corresponding_journal_authors.csv' AS row
                WITH row
                    MERGE (a:Author { name: row[1] })
                    WITH row, a
                        MATCH (p:Paper { key: row[0] })
                        CREATE (a)-[:WRITE]->(p)
                        RETURN a
            """)
            print('Non-corresponding journal authors loaded.')

    def generate_random_citations(self):
        print('Generating random citations between papers...')
        with self.driver.session() as session:
            session.run("""
                MATCH (p1:Paper)
                    WITH p1
                    MATCH (p2:Paper) WHERE p1 <> p2 AND rand() < 0.01
                        MERGE (p1)-[:CITED_BY]->(p2)
                        RETURN p1, p2
            """)
            print('Citations generated.')

    def load_schools(self):
        print('Loading schools...')
        with self.driver.session() as session:
            session.run("""
                MATCH (o:Organization) DETACH DELETE o
            """)
            session.run("""
                LOAD CSV FROM 'file:///schools.csv' AS row
                WITH row
                    CREATE (o:Organization { name: row[0] })
                    RETURN o
            """)
            session.run('CREATE INDEX ON :Organization(name)')
            print('Schools loaded.')

    def load_author_schools(self):
        print('Loading author schools...')
        with self.driver.session() as session:
            session.run("""
                MATCH (:Author)-[aw:AFFILIATED_WITH]->(:Organization) DETACH DELETE aw
            """)
            session.run("""
                LOAD CSV FROM 'file:///author_schools.csv' AS row
                WITH row
                    MATCH (a:Author), (o:Organization)
                    WHERE a.name = row[0]
                    AND o.name = row[1]
                    WITH row, a, o
                        CREATE (a)-[:AFFILIATED_WITH]->(o)
                        RETURN a, o
            """)
            print("Author's affiliations loaded.")

    def set_num_of_reviewers(self):
        print('Setting number of reviewers to conferences and journals...')
        with self.driver.session() as session:
            session.run("""
                MATCH (x)
                WHERE x:Conference OR x:Journal
                SET x.num_of_reviewers = 3
                RETURN x
            """)
        print('Number of reviewers to conferences and journals have been set.')

    def load_initial_conference_paper_reviews(self):
        print('Loading conference paper reviewers...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///conference_paper_reviewers.csv' AS row
                    MATCH (p:Paper), (a:Author)
                    WHERE p.key = row[0]
                    AND a.name = row[1]
                        WITH p, a, row
                        MERGE (a)-[:REVIEW]->(p)
                        RETURN a, p
            """)
        print('Conference paper reviewers loaded.')

    def load_initial_journal_paper_reviews(self):
        print('Loading journal paper reviewers...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///conference_paper_reviewers.csv' AS row
                    MATCH (p:Paper), (a:Author)
                    WHERE p.key = row[0]
                    AND a.name = row[1]
                        WITH p, a, row
                        MERGE (p)<-[:REVIEW]-(a)
                        RETURN p, a
            """)
        print('Conference paper reviewers loaded.')

    def load_evolve_conference_paper_reviews(self):
        print('Loading suggested decision and textual description to reviews...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///conference_paper_reviewers.csv' AS row
                    MATCH (p:Paper), (a:Author)
                    WHERE p.key = row[0]
                    AND a.name = row[1]
                        WITH p, a, row
                        MATCH (a)-[r:REVIEW]->(p)
                        SET r.accept = true
                        SET r.textual_description = row[2]
                        RETURN a, p
            """)
        print('Conference paper reviewers loaded.')

    def load_evolve_journal_paper_reviews(self):
        print('Loading journal paper reviewers...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///conference_paper_reviewers.csv' AS row
                    MATCH (p:Paper), (a:Author)
                    WHERE p.key = row[0]
                    AND a.name = row[1]
                        WITH p, a, row
                        MATCH (a)-[r:REVIEW]->(p)
                        SET r.accept = true
                        SET r.textual_description = row[2]
                        RETURN p, a
            """)
        print('Conference paper reviewers loaded.')
