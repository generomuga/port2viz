from sqlite3.dbapi2 import DatabaseError
from lib.database import Database
from lib.scraper import Scraper

import os
import time
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

if __name__ == '__main__':

    # Set required paths and attributes
    ROOT = os.path.abspath(os.curdir)
    DRIVER_PATH = ROOT+'/driver/chromedriver.exe'
    DB_PATH = ROOT+'/db/kdm_dev_try.db'
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

    ports = []
    id = 0

    for locinfo in locinfos:
        country, locode = str(locinfo).split()

        locode_url = set_locode_url(
            base_url = BASE_URL_LOCODE,
            country_code = country.lower(),
            extension = EXTENSION
        )

        page_content = scp.get_page_content(
            url=locode_url
        )

        rows = page_content.find_all("tr")

        for row in rows:
            dict_port = {}
            if country+'  '+locode in str(row).strip():
                id += 1
                cols = row.find_all("td")
                unlocode = cols[1].string
                port_name = cols[2].string
                function = cols[5].string
                coordinates = cols[9].string

                dict_port = {
                    "id": id,
                    "port_name": port_name,
                    "unlocode": unlocode,
                    "country_name": "Philippines",
                    "function": function,
                    "coordinates": coordinates
                }
    
                ports.append(dict_port)

    cur.executemany(
        "INSERT INTO port (port_name,country_name,unlocode,function,coordinates) VALUES (:port_name,:country_name,:unlocode,:function,:coordinates)",
        ports
    )
    con.commit()

    # for port in ports:
    #     print (port['id'])