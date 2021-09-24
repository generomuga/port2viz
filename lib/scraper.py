from bs4 import BeautifulSoup
import requests
import logging

class Scraper:

    def __init__(self):
        pass

    def get_page_content(self, **kwargs):
        url = kwargs['url']

        try:
            page = requests.get(url)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, 'html5lib')
                logging.info('Page content: '+soup.prettify())
                return soup
        except Exception as err:
            print (err)