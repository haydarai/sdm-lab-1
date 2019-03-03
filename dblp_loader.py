import pandas as pd
from nameparser import HumanName


def is_corresponding(author):
    last_name = author.last_name.split()
    if last_name:
        return last_name[-1] in author.key
    return False


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

        df['last_name'] = df['author'].apply(lambda name: HumanName(name).last)
        df['is_corresponding'] = df.apply(is_corresponding, axis=1)

        df.to_csv('datasets/minimized_conference_authors.csv',
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

        df['last_name'] = df['author'].apply(lambda name: HumanName(name).last)
        df['is_corresponding'] = df.apply(is_corresponding, axis=1)

        df.to_csv('datasets/minimized_journal_authors.csv',
                  sep=',', index=False, header=False)
        print('Authors from journal papers extracted.')
