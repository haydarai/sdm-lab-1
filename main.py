from neo4j_loader import Neo4J_Loader
from dblp_loader import DBLP_Loader

if __name__ == "__main__":

    file_loader = DBLP_Loader()
    file_loader.extract_conferences()
    file_loader.extract_journals()
    file_loader.extract_conference_papers()
    file_loader.extract_journal_papers()
    file_loader.extract_conference_authors()
    file_loader.extract_journal_authors()

    database_loader = Neo4J_Loader()
    database_loader.load_conferences()
    database_loader.load_journals()
    database_loader.delete_papers()
    database_loader.load_conference_papers()
    database_loader.load_journal_papers()
    database_loader.load_conference_authors()
    database_loader.load_journal_authors()
    database_loader.generate_random_citations()
