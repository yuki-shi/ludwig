import argparse
from gupyuki import get_job_listings, format_to_dataframe
from induki import Induki
import pandas as pd
import datetime as dt
import sqlite3
import os
import re

def get_gupy_dataframe(keyword: str) -> pd.DataFrame:
    # Get gupy job listings as a dataframe
    jobs = get_job_listings(keyword)
    df = format_to_dataframe(jobs)

    # Check if the dataframe is empty
    try:
        # Add data source and queried keyword as attributes
        df['source'] = 'gupy'
        df['keyword'] = keyword

    except TypeError:
        print('No jobs on Gupy')
        return

    return df

def get_indeed_dataframe(keyword: str) -> pd.DataFrame:
    # Get indeed job listings as a dict
    induki = Induki(keyword)
    jobs = induki.scrape_page_source()
    df = pd.DataFrame([jobs]).transpose().reset_index() 

    # Check if the dict is empty
    try:
        # Normalize column names
        df = df.rename(columns={0: 'url', 'index': 'job'})
        # Add today's date as an attribute to Indeed's dataframe
        df['date'] = dt.datetime.today().strftime('%d/%m/%Y')
        # Add data source and queried keyword as attribute
        df['source'] = 'indeed'
        df['keyword'] = keyword

    except TypeError:
        print('No jobs on Indeed')
        return

    return df

def concat_dataframes(df_gupy: pd.DataFrame, df_indeed: pd.DataFrame) -> pd.DataFrame:
    # Check if the dataframes are empty
    if df_gupy is None:
        return df_indeed
    elif df_indeed is None:
        return df_gupy

    # Outer join on both dataframes row-wise 
    df_final = pd.concat([df_gupy, df_indeed], axis=0)
    df_final = df_final.reset_index(drop=True)

    return df_final

def connect_to_db(func):
    # Connect and close connection to SQLite
    def wrapper(*args, **kwargs):
        conn = sqlite3.connect('jobs.db')
        func(conn, *args, **kwargs)
        conn.commit()
        conn.close()

    return wrapper

@connect_to_db
def update_to_db(conn, df: pd.DataFrame) -> None:
    # Insert the aggregated dataframe to SQLite
    return df.to_sql(name='job_listings',
                     con=conn,
                     if_exists='append')

@connect_to_db
def show_db(conn) -> None:
    # Check all values stored in the DB
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM job_listings')
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    return


if __name__ == '__main__':
    
    # Maybe store the keywords into a file, so we won't need to run a shellscript everyday
    # TODO: if only one keyword, there is no need to split
    KEYWORDS = [x.split(',') for x in os.getenv('KEYWORDS').split(' ')]
    if KEYWORDS is None:
        raise TypeError('No KEYWORDS environmental variable found')

    # For testing
    keyword = KEYWORDS[0][1]

    df_gupy = get_gupy_dataframe(keyword)
    df_indeed = get_indeed_dataframe(keyword)
    df_agg = concat_dataframes(df_gupy, df_indeed)
    update_to_db(df_agg)
    show_db()
