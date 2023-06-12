import argparse
from gupyuki import get_job_listings, format_to_dataframe
from induki import Induki
import pandas as pd
import datetime as dt

parser = argparse.ArgumentParser(description='')
parser.add_argument('--keyword',
                    '-k',
                    type=str,
                    required=True)
args = parser.parse_args()
keyword = args.keyword

def get_gupy_dataframe(keyword):
    # Get gupy job listings as a dataframe
    jobs = get_job_listings(keyword)
    # TODO: Check if empty list
    df = format_to_dataframe(gupy_jobs)

    return df

def get_indeed_dataframe(keyword):
    # Get indeed job listings as a dict
    induki = Induki(keyword)
    jobs = induki.scrape_page_source()

    # TODO: Check if empty dict

    # Transform into a dataframe 
    df = pd.DataFrame([jobs]).transpose().reset_index() 
    df = df.rename(columns={0: 'url', 'index': 'job'})
    # Add today's date as an attribute to Indeed's dataframe
    df['date'] = dt.datetime.today().strftime('%d/%m/%Y')

    return df

def concat_dataframes(df_gupy, df_indeed):
    # Add each data source as an attribute
    for df, source in zip([df_gupy, df_indeed], ['gupy', 'indeed']):
        df['source'] = source

    # Add queried keyword as an attribute
    for df in [df_gupy, df_indeed]:
        df['keyword'] = keyword

    # Outer join on both dataframes 
    df_final = pd.concat([df_gupy, df_indeed], axis=0)

    return df_final


df_gupy = get_gupy_dataframe(keyword)
df_indeed = get_indeed_dataframe(keyword)
df_agg = concat_dataframes(df_gupy, df_indeed)
