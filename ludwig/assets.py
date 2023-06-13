from dagster import asset, get_dagster_logger, RetryPolicy 
import os
from . import etl

@asset
def init():
    keyword = 'python'
#    KEYWORDS = [x.split(',') for x in os.getenv('KEYWORDS').split(' ')]
#    if KEYWORDS is None:
#        raise TypeError('No KEYWORDS environmental variable found')

    # FOR TESTING AAAA
#    keyword = KEYWORDS[0][1]
    return keyword

@asset
def gupy_source_data(init):
    logger = get_dagster_logger()
    df = etl.get_gupy_dataframe(init)

    if df is None:
        logger.info('No jobs on Gupy so far')

    return df 

# Nesse pode precisar de retry por conta do selenium
@asset
def indeed_source_data(init):
    logger = get_dagster_logger()

    df = etl.get_indeed_dataframe(init)
    
    if df is None:
        logger.info('No jobs on Indeed so far')

    return df 

@asset
def outer_join(gupy_source_data, indeed_source_data):
    return etl.concat_dataframes(gupy_source_data, indeed_source_data)
