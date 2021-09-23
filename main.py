from lib.database import Database
from lib.scraper import Scraper

import os
import numpy as np

def convert_to_list(results):
    list = []
    for result in results:
        list.append(result)
    return list

def set_locode_url(**kwargs):
    base_url = kwargs['base_url']
    country_code = kwargs['country_code']
    extension = kwargs['extension']
    return base_url+country_code+extension

def get_distinct_location(locations):
    return np.unique(locations)

def post_port_data(data):
    try:
        cur.executemany(
            "INSERT INTO port (port_name,country_name,unlocode,function,coordinates) VALUES (:port_name,:country_name,:unlocode,:function,:coordinates)",
            data
        )
        con.commit()    
    except Exception as err:
        con.rollback()

def get_country_name(content, country_code):
    rows = content.find_all("tr")
    for row in rows:
        if country_code in str(row).strip():
            cols = row.find_all("td")
            country_name = cols[1].string
            return country_name

def is_failed_mapping(**kwargs):
    function = kwargs['function']
    country = kwargs['country_name']
    port = kwargs['port_name']
    coordinates = kwargs['coordinates']

    if '1' not in function:
        return 1
    if len(country) <= 0 or str(country) == '':
        return 1
    if len(port) <= 0 or str(port) == '':
        return 1
    if len(coordinates) <= 0 or str(coordinates) == '':
        return 1

    return 0

if __name__ == '__main__':

    # Set required paths and attributes
    ROOT = os.path.abspath(os.curdir)
    DRIVER_PATH = ROOT+'/driver/chromedriver.exe'
    DB_PATH = ROOT+'/db/kdm_dev_try.db'
    BASE_URL = 'https://unece.org/trade/cefact/unlocode-code-list-country-and-territory'
    BASE_URL_LOCODE = 'https://service.unece.org/trade/locode/'
    EXTENSION = '.htm'

    # Initialize db class and components
    db = Database()
    con = db.connect(db=DB_PATH)
    cur = con.cursor()
    query = "SELECT unlocode FROM cargoline ORDER BY unlocode;"

    # Initialize scraper class
    scp = Scraper()

    # Get locode data
    results = db.get_data(cursor=cur, query_str=query)

    # Convert the db results to a list and get the distinct values
    list_location = convert_to_list(results)
    locinfos = get_distinct_location(list_location)

    # Initialize variable that will handle port data and id
    ports = []
    id = 0

    # Get home page content
    home_page_content = scp.get_page_content(
        url=BASE_URL
    )

    for locinfo in locinfos:
        # Get country and location code
        country_code, locode = str(locinfo).split()

        # Set location code URL
        locode_url = set_locode_url(
            base_url = BASE_URL_LOCODE,
            country_code = country_code.lower(),
            extension = EXTENSION
        )

        # Get page content for each url call
        locode_page_content = scp.get_page_content(
            url=locode_url
        )

        # Get all data in <tr> elements
        rows = locode_page_content.find_all("tr")

        for row in rows:
            # Intialize dictionary port data
            dict_port = {}

            # Check if the country and locode are existing in the row
            if country_code+'  '+locode in str(row).strip():
                id += 1
                cols = row.find_all("td")
                unlocode = country_code+'  '+locode
                country_name = get_country_name(home_page_content, country_code)
                port_name = cols[2].string
                function = cols[5].string
                coordinates = cols[9].string

                # Set dictionary port data
                dict_port = {
                    "id": id,
                    "port_name": port_name,
                    "unlocode": unlocode,
                    "country_name": country_name,
                    "function": function,
                    "coordinates": coordinates
                }
    
                # Save to list of port data
                ports.append(dict_port)

    # Save to database
    post_port_data(ports)