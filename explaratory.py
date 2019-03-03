#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 26 14:28:36 2019

@author: haydar
"""

import pandas as pd

df = pd.read_json('datasets/dblp-ref-0.json', lines=True, nrows=1000)

#df_article = pd.read_csv('datasets/output_article.csv', delimiter = ';',
#                         nrows = 1000, error_bad_lines = False)
#df_article = df_article.dropna(axis = 1, how = 'all')
# df_article = df_article[['journal', 'year', 'volume', 'month', 'mdate']]
#df_article = df_article.sort_values(by=['mdate'], ascending=False, na_position='last')
#df_article = df_article.drop_duplicates(['journal', 'year', 'volume'], keep='first')
#df_article = df_article.drop('mdate', axis = 1)

#df = pd.read_csv('datasets/output_inproceedings.csv',
#                             delimiter = ';', nrows = 10000,
#                             error_bad_lines = False)
#df = df[['author', 'key']]
#df = df.drop_duplicates()

#cols = df.columns.difference(['author'])
#df['author'] = df['author'].str.split('|')

#df = df.set_index(['key']).author.apply(pd.Series).stack()\
#reset_index(name = 'author').drop('level_1', axis = 1)

#df['last_name'] = df['author'].apply(lambda name: HumanName(name).last)

#df = df.dropna(axis=1, how='all')

#def is_corresponding(author):
#    return author.last_name in author.key

#df['is_corresponding'] = df.apply(is_corresponding, axis=1)


#df =  (df.loc[df.index.repeat(authors.str.len()), cols]
#         .assign(authors=list(chain.from_iterable(authors.tolist()))))

#new_df = pd.DataFrame(df_authors.author.str.split('|').tolist(), index=df_authors.key).stack()
#new_df = new_df.reset_index([0, 'key'])
#df_proceedings = df_proceedings.dropna(axis = 1, how = 'all')
#df_proceedings = df_proceedings[['booktitle', 'year', 'month']]
#df_proceedings = df_proceedings.drop_duplicates()
#df_proceedings = df_proceedings.dropna(subset=['booktitle'])
#df_proceedings = df_proceedings[np.isfinite(df['booktitle'])]
#df_proceedings = df_proceedings(pd.notna(df_proceedings['booktitle']))

#df_author = pd.read_csv('datasets/authors.csv', delimiter = ';', nrows = 1000,
#                        encoding = "ISO-8859-1")

#df_article_2 = pd.read_csv('datasets/articles.csv', delimiter = ';', nrows = 10,
#                        encoding = "ISO-8859-1")

#df_in_proceedings = pd.read_csv('datasets/output_inproceedings.csv',
#                                delimiter = ';', nrows = 1000,
#                                error_bad_lines = False)

#df = pd.read_csv('datasets/output_article.csv', delimiter=';', nrows=10000, error_bad_lines=False)

# Drop columns with no value
#df = df.dropna(axis=1, how='all')
#df_papers = df[['key', 'title']]