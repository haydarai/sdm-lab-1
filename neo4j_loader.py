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
                LOAD CSV FROM 'file:///minimized_proceedings.csv' AS row
                WITH row
                    WITH toString(toInteger(row[1])) + '-01-01' AS startDate, row
                        WITH toString(toInteger(row[1])) + '-01-02' AS endDate, startDate, row
                            CREATE (c:Conference { title: row[0], startDate: startDate, endDate: endDate })
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
                LOAD CSV FROM 'file:///minimized_journals.csv' AS row
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

    def delete_papers(self):
        with self.driver.session() as session:
            session.run("""
                MATCH (p:Paper) DETACH DELETE p
            """)

    def load_conference_papers(self):
        print('Loading conference papers to Neo4J...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///minimized_conference_papers.csv' AS row
                WITH row
                    CREATE (p:Paper { key: row[0], title: row[1], abstract: row[4] })
                    WITH row, p
                        MATCH (c:Conference { title: row[2], startDate: toString(toInteger(row[3])) + '-01-01' })
                        CREATE (p)-[:PUBLISHED_IN]->(c)
                        RETURN p
            """)
            print('Conference papers loaded.')

    def load_journal_papers(self):
        print('Loading journal papers to Neo4J...')
        with self.driver.session() as session:
            session.run("""
                LOAD CSV FROM 'file:///minimized_journal_papers.csv' AS row
                WITH row
                    CREATE (p:Paper { key: row[0], title: row[1], abstract: row[5] })
                    WITH row, p
                        MATCH (j:Journal { title: row[2], date: toString(toInteger(row[3])) + '-01-01', volume: row[4] })
                        CREATE (p)-[:PUBLISHED_IN]->(j)
                        RETURN p
            """)
            print('Journal papers loaded.')

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
                LOAD CSV FROM 'file:///minimized_conference_venues.csv' AS row
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
                LOAD CSV FROM 'file:///minimized_corresponding_conference_authors.csv' AS row
                WITH row
                    CREATE (a:Author { name: row[1] })
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
                LOAD CSV FROM 'file:///minimized_corresponding_journal_authors.csv' AS row
                WITH row
                    CREATE (a:Author { name: row[1] })
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
                LOAD CSV FROM 'file:///minimized_non_corresponding_conference_authors.csv' AS row
                WITH row
                    CREATE (a:Author { name: row[1] })
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
                LOAD CSV FROM 'file:///minimized_non_corresponding_journal_authors.csv' AS row
                WITH row
                    CREATE (a:Author { name: row[1] })
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
