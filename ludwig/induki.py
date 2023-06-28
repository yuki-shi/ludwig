from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
from time import sleep
from typing import Dict
import re


class Induki():
    def __init__(self, keyword: str):
        # Check if keyword has blankspaces
        if ' ' in keyword:
            self.keyword = re.sub(' ', '+', keyword)
        self.keyword = keyword
        self.driver = self.init_driver()

    # Selenium init
    def init_driver(self) -> webdriver.Chrome:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--incognito')
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'    
        options.add_argument(f'user-agent={user_agent}')
        return webdriver.Chrome(options=options)

    # Get keyword regex
    def get_regex_keyword(self) -> str:
        regex_keyword = ''
        for letter in self.keyword.lower():
            regex_keyword += f'[{letter}{letter.upper()}]'

        return f'.*{regex_keyword}.*'

    # Get page source
    def get_page_source(self) -> str: 
        # TODO: try/except
        url = f'https://br.indeed.com/empregos?q={self.keyword}&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=1&vjk=2d04ee1249661eef'
        self.driver.get(url)

        return self.driver.page_source

# Scrape for job listing
    def scrape_page_source(self) -> Dict[str, str]:
        pg_source = self.get_page_source()
        soup = BeautifulSoup(pg_source, 'html.parser')
        jobs = soup.find_all('h2')

        output_dict = {}
        for job in jobs:
            output_dict[job.text] = f'http://br.indeed.com{job.find_all("a")[0]["href"]}'

        # Filter by keyword
        regex_keyword = self.get_regex_keyword()
        output_dict = {key: value for key, value in output_dict.items() if re.match(regex_keyword, key)}

        return output_dict
