import pandas as pd
import re

from datetime import datetime
from ural import get_domain_name
from utils import import_data_str

pd.set_option('display.max_colwidth', None)

def merge_notes_tweets(filename_1, filename_2):

    df = import_data_str(filename_1)
    df['date_note'] = pd.to_datetime(df['createdAtMillis'], unit='ms').dt.date

    df_tw = import_data_str(filename_2)
    df_tw = df_tw.rename(columns={'id': 'tweetId'})

    df_merge = df.merge(df_tw, how = 'inner', on = ['tweetId'])

    return df_merge

def get_multiple_urls(summary):

    list_urls =[]

    for url in re.findall('(https://\S+)', summary):
        list_urls.append(url)

    return list_urls

def get_list_domain_names(list_urls):

    list_domains = []
    for item in list_urls:
        a = get_domain_name(item)
        list_domains.append(a)

    return list_domains

def remove_test_notes(df):

    df= df[~df['author_name'].isin(['birdwatch'])]
    df= df[~df['summary'].isin(['test', 'Test'])]

    return df

def arrange_merged_df(df):

    df = remove_test_notes(df)

    df['summary'] = df['summary'].astype(str)
    df = df[~df['summary'].isin(['nan'])]

    df['length_summary_note'] = df['summary'].apply(len)
    df['summary_notes_links'] = df['summary'].apply(get_multiple_urls)
    df['summary_notes_domains'] = df['summary_notes_links'].apply(get_list_domain_names)
    df['summary_notes_number_of_links'] = df['summary_notes_links'].apply(len)
    df= df.sort_values(by = 'summary_notes_number_of_links')

    return df

def main():

    df = merge_notes_tweets(filename_1 = 'notes-00000.csv',
                            filename_2 = 'tweet_metrics_notes_2022_05_02_with_links.csv')

    df_merge = arrange_merged_df(df)
    df_merge.to_csv('./data/merged_notes_tweets.csv')

if __name__ == '__main__':

    main()
