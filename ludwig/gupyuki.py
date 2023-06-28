#!./env/bin/python

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import sys


def get_job_listings(keyword: str) -> list[str, str, str]:
    """
    Get Gupy job-listings based on a given keyword

    Args:
        keyword (str): Keyword to be queried on Gupy
        
    Returns:
        list: List with matched job-listings.
              Each element has the following fields:
              - Job title
              - Published date
              - URL
    """
    response = ((requests.get(f'https://portal.gupy.io/job-search/term={keyword}')
                         .text))
    soup = BeautifulSoup(response, 'html.parser')
    if not soup.find('h4'):
        sys.exit('No results found, try another keyword')

    vagas = []
    list_items = soup.find_all('li')

    for item in list_items:
        vagas.append([item.find('h4').text,
                      item.find_all('p')[-1].text,
                      item.find_all('a')[0]['href']])
    return vagas

def format_to_dataframe(job_list: list[str, str, str]) -> pd.DataFrame:
    """
    Transforms returned list from get_job_listings() into a DataFrame for basic cleaning and manipulation.

    Args: job_list(list): Job-listing returned from get_job_listings().

    Returns:
        string: Job-listings that were published today.
    """
    df = pd.DataFrame(job_list, columns=['job', 'date', 'url'])
    df['date'] = df['date'].str.replace(r'.*(?<= )', '', regex=True)
    today = dt.datetime.today().strftime('%d/%m/%Y')

    if not df['date'].str.contains(today).any():
        return

    return df.loc[df['date'] == today, :]
