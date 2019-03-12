from dotenv import load_dotenv
import argparse
from neo4j_loader import Neo4J_Loader
from dblp_loader import DBLP_Loader
from query_runner import Query_Runner

load_dotenv()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--parse', action='store_true')
    parser.add_argument('--load', action='store_true')
    parser.add_argument('--evolve', action='store_true')
    parser.add_argument('--recommend')
    parser.add_argument('--gurus', type=int)
    args = parser.parse_args()

    if args.parse and not args.evolve:
        file_loader = DBLP_Loader()
        file_loader.extract_conferences()
        file_loader.extract_journals()
        file_loader.extract_conference_venues()
        file_loader.extract_conference_papers()
        file_loader.extract_journal_papers()
        file_loader.extract_conference_authors()
        file_loader.extract_journal_authors()
        file_loader.generate_random_conference_reviewers()
        file_loader.generate_random_journal_reviewers()
        print("Copy files generated in 'output' folder to '/var/lib/neo4j/import'")
    elif args.load and not args.evolve:
        database_loader = Neo4J_Loader()
        database_loader.load_conferences()
        database_loader.add_index_to_conferences()
        database_loader.load_journals()
        database_loader.add_index_to_journals()
        database_loader.load_conference_venues()
        database_loader.set_num_of_reviewers()
        database_loader.delete_papers()
        database_loader.load_conference_papers()
        database_loader.load_journal_papers()
        database_loader.add_index_to_papers()
        database_loader.load_conference_paper_keywords()
        database_loader.load_journal_paper_keywords()
        database_loader.add_index_to_authors()
        database_loader.load_corresponding_conference_authors()
        database_loader.load_corresponding_journal_authors()
        database_loader.load_non_corresponding_conference_authors()
        database_loader.load_non_corresponding_journal_authors()
        database_loader.generate_random_citations()
        database_loader.load_initial_conference_paper_reviews()
        database_loader.load_initial_journal_paper_reviews()
        print('All data loaded.')
    elif args.parse and args.evolve:
        file_loader = DBLP_Loader()
        file_loader.extract_schools()
        print("Copy files generated in 'output' folder to '/var/lib/neo4j/import'")
    elif args.load and args.evolve:
        file_loader = DBLP_Loader()
        database_loader = Neo4J_Loader()
        database_loader.load_schools()
        file_loader.generate_random_author_schools()
        database_loader.load_author_schools()
        database_loader.load_evolve_conference_paper_reviews()
        database_loader.load_evolve_journal_paper_reviews()
        print('All data loaded.')
    elif args.recommend and args.gurus:
        keywords = """['data', 'database', 'management', 'olap', 'postgres', 'xml', 'relational', 'queries', 'sql']"""
        query_runner = Query_Runner()
        publications = query_runner.get_publication_communities(args.recommend)
        papers = query_runner.get_top_papers(publications)
        gurus = query_runner.get_gurus(papers, args.gurus)
        print('Gurus:')
        print(gurus)
    elif args.recommend:
        query_runner = Query_Runner()
        publications = query_runner.get_publication_communities(args.recommend)
        papers = query_runner.get_top_papers(publications)
        authors = query_runner.get_authors(papers)
        print('Recommended reviewers:')
        print(authors)
