from bs4 import BeautifulSoup
import requests

class Scraper:

    def __init__(self):
        pass

    def navigate(self, **kwargs):
        url = kwargs['url']
        driver_path = kwargs['driver_path']

        driver = webdriver.Chrome(driver_path)
        driver.get(url)

    def get_page_content(self, **kwargs):
        url = kwargs['url']

        try:
            page = requests.get(url)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, 'html5lib')
                print ('Get page content...')
                return soup
        except Exception as err:
            print (err)