from dagster import asset, get_dagster_logger, RetryPolicy 
import csv
import pandas as pd
from . import etl

# Abstraction of the iteration of the extraction function on every keyword
def iterate_over_keywords(func: callable, keywords: list) -> pd.DataFrame:
    logger = get_dagster_logger()
    dfs = [] 
    
    # Call the extract function for each keyword
    for keyword in keywords:
        logger.info(f'Running {keyword} on {func.__name__}')
        df = func(keyword)
        dfs.append(df)
    
    # Remove None values from aggregated results
    dfs = [x for x in dfs if x is not None] 

    # If there are more than one DataFrame, concatenate them
    if len(dfs) >= 2:
        df_final = pd.concat(dfs, axis=0, ignore_index=True)

    # If not, return the only DataFrame (or None) as result
    else:
        df_final = dfs[0]

    return df_final
    
# If it's the first run of the day, drop tables

@asset(non_argument_deps={'keywords_csv'})
def init() -> list:
    with open('keywords.csv', 'r') as f:
        keywords = list(csv.reader(f, delimiter=','))
    return keywords[0]

@asset
def gupy_source_data(init):
    df = iterate_over_keywords(etl.get_gupy_dataframe, init) 
    return df
    
# Nesse pode precisar de retry por conta do selenium
@asset
def indeed_source_data(init):
    df = iterate_over_keywords(etl.get_indeed_dataframe, init)
    return df 

@asset
def outer_join(gupy_source_data, indeed_source_data):
    return etl.concat_dataframes(gupy_source_data, indeed_source_data)

@asset
def load_to_db(outer_join):
        return etl.update_to_db(outer_join) 

# If it's the last run of the day, append to data warehouse
