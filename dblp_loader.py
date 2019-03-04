import pandas as pd
import re
import lorem
from gensim.utils import deaccent
from nameparser import HumanName


def generate_abstract(row):
    return lorem.paragraph()


def is_corresponding(author):
    last_name = author.last_name.split()
    if last_name:
        return deaccent(last_name[-1]) in deaccent(author.key)
    return False


def extract_last_name(full_name):
    full_name = re.sub(r'\d+', '', full_name)
    return HumanName(full_name).last


def remove_numbers_from_name(name):
    return re.sub(r'\d+', '', name)


class DBLP_Loader():

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

    def extract_conferences(self):
        print('Extracting conferences...')
        df = pd.read_csv('datasets/output_inproceedings.csv',
                         delimiter=';', nrows=10000, error_bad_lines=False)

        # Drop columns with no value
        df = df.dropna(axis=1, how='all')

        # Extract useful columns
        df = df[['booktitle', 'year']]

        # Drop rows with incomplete information
        df = df.drop_duplicates()

        # Ignoring rows with non-numerical value in year column
        df['year'] = pd.to_numeric(df['year'], errors='coerce')

        # Drop rows with any null value in defined columns
        df = df.dropna(subset=['booktitle', 'year'])

        df.to_csv('datasets/minimized_proceedings.csv',
                  sep=',', index=False, header=False)
        print('Conferences extracted.')

    def extract_journals(self):
        print('Extracting journals...')
        df = pd.read_csv('datasets/output_article.csv',
                         delimiter=';', nrows=10000, error_bad_lines=False)
        df = df.dropna(axis=1, how='all')
        df = df[['journal', 'year', 'volume', 'mdate']]

        # Ignoring rows with non-numerical value in year column
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df = df.sort_values(by=['mdate'], ascending=False, na_position='last')
        df = df.drop_duplicates(['journal', 'year', 'volume'], keep='first')
        df = df.drop('mdate', axis=1)

        # Drop rows with any null value in defined columns
        df = df.dropna(subset=['journal', 'year', 'volume'])

        df.to_csv('datasets/minimized_journals.csv',
                  sep=',', index=False, header=False)
        print('Journals extracted.')

    def extract_conference_papers(self):
        print('Extracting conference papers...')
        df = pd.read_csv('datasets/output_inproceedings.csv',
                         delimiter=';', nrows=10000, error_bad_lines=False)

        # Drop columns with no value
        df = df.dropna(axis=1, how='all')

        # Extract useful columns
        df = df[['key', 'title', 'booktitle', 'year', 'mdate']]

        df = df.sort_values(by=['mdate'], ascending=False, na_position='last')
        df = df.drop_duplicates(['key', 'title'], keep='first')
        df = df.drop('mdate', axis=1)

        # Drop rows with any null value in defined columns
        df = df.dropna(subset=['key', 'title', 'booktitle', 'year'])

        # Generate random abstract
        df['abstract'] = df.apply(generate_abstract, axis=1)

        df.to_csv('datasets/minimized_conference_papers.csv',
                  sep=',', index=False, header=False)
        print('Conference papers extracted.')

    def extract_journal_papers(self):
        print('Extracting journal papers...')
        df = pd.read_csv('datasets/output_article.csv',
                         delimiter=';', nrows=10000, error_bad_lines=False)

        # Drop columns with no value
        df = df.dropna(axis=1, how='all')

        # Extract useful columns
        df = df[['key', 'title', 'journal', 'year', 'volume', 'mdate']]

        df = df.sort_values(by=['mdate'], ascending=False, na_position='last')
        df = df.drop_duplicates(['key', 'title'], keep='first')
        df = df.drop('mdate', axis=1)

        # Drop rows with any null value in defined columns
        df = df.dropna(subset=['key', 'title', 'journal', 'year', 'volume'])

        # Generate random abstract
        df['abstract'] = df.apply(generate_abstract, axis=1)

        df.to_csv('datasets/minimized_journal_papers.csv',
                  sep=',', index=False, header=False)
        print('Journal papers extracted.')

    def extract_conference_authors(self):
        print('Extracting authors from conference papers...')
        df = pd.read_csv('datasets/output_inproceedings.csv',
                         delimiter=';', nrows=10000,
                         error_bad_lines=False)

        # Drop columns with no value
        df = df.dropna(axis=1, how='all')

        # Extract useful columns
        df = df[['author', 'key']]
        df = df.drop_duplicates()

        df['author'] = df['author'].str.split('|')

        df = df.set_index(['key']).author.apply(pd.Series).stack(
        ).reset_index(name='author').drop('level_1', axis=1)

        df['author'] = df['author'].apply(remove_numbers_from_name)
        df['last_name'] = df['author'].apply(extract_last_name)
        df['is_corresponding'] = df.apply(is_corresponding, axis=1)

        df_corresponding = df[df['is_corresponding'] == True]
        df_non_corresponding = df[df['is_corresponding'] == False]

        # Extract useful columns
        df_corresponding = df_corresponding[['key', 'author']]
        df_non_corresponding = df_non_corresponding[['key', 'author']]

        # Drop duplicates
        df_corresponding = df_corresponding.drop_duplicates(
            ['key'], keep='first')
        df_non_corresponding = df_non_corresponding.drop_duplicates(
            ['key', 'author'], keep='first')

        df_corresponding.to_csv('datasets/minimized_corresponding_conference_authors.csv',
                                sep=',', index=False, header=False)
        df_non_corresponding.to_csv('datasets/minimized_non_corresponding_conference_authors.csv',
                                    sep=',', index=False, header=False)
        print('Authors from conference papers extracted.')

    def extract_journal_authors(self):
        print('Extracting authors from journal papers...')
        df = pd.read_csv('datasets/output_article.csv',
                         delimiter=';', nrows=10000,
                         error_bad_lines=False)

        # Drop columns with no value
        df = df.dropna(axis=1, how='all')

        # Extract useful columns
        df = df[['author', 'key']]
        df = df.drop_duplicates()

        df['author'] = df['author'].str.split('|')

        df = df.set_index(['key']).author.apply(pd.Series).stack(
        ).reset_index(name='author').drop('level_1', axis=1)

        df['author'] = df['author'].apply(remove_numbers_from_name)
        df['last_name'] = df['author'].apply(extract_last_name)
        df['is_corresponding'] = df.apply(is_corresponding, axis=1)

        df_corresponding = df[df['is_corresponding'] == True]
        df_non_corresponding = df[df['is_corresponding'] == False]

        # Extract useful columns
        df_corresponding = df_corresponding[['key', 'author']]
        df_non_corresponding = df_non_corresponding[['key', 'author']]

        # Drop duplicates
        df_corresponding = df_corresponding.drop_duplicates(
            ['key'], keep='first')
        df_non_corresponding = df_non_corresponding.drop_duplicates(
            ['key', 'author'], keep='first')

        df_corresponding.to_csv('datasets/minimized_corresponding_journal_authors.csv',
                                sep=',', index=False, header=False)
        df_non_corresponding.to_csv('datasets/minimized_non_corresponding_journal_authors.csv',
                                    sep=',', index=False, header=False)

        df.to_csv('datasets/minimized_journal_authors.csv',
                  sep=',', index=False, header=False)
        print('Authors from journal papers extracted.')
