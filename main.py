from lib.database import Database
from lib.scraper import Scraper

import os
import numpy as np

import random

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
    print ('Getting list of unlocode from db...')
    results = db.get_data(cursor=cur, query_str=query)

    # Convert the db results to a list and get the distinct values
    list_location = convert_to_list(results)
    locinfos = get_distinct_location(list_location)

    # Initialize variable that will handle port data and id
    ports = []
    id = 0

    # Get home page content
    print ('Getting home page content...')
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
                # is_failed = is_failed_mapping(
                #     function=function,
                #     country_name=country_name,
                #     port_name=port_name,
                #     coordinates=coordinates
                # )
                is_failed = random.randrange(0,2)
                print (is_failed)

                # Set dictionary port data
                dict_port = {
                    "id": id,
                    "port_name": port_name,
                    "unlocode": unlocode,
                    "country_name": country_name,
                    "function": function,
                    "coordinates": coordinates,
                    # "is_failed_mapping": is_failed
                    "is_failed_mapping": is_failed
                }

                # Save to list of port data
                ports.append(dict_port)

    # Save to database
    query_save_port = """
        INSERT INTO port 
            (
                port_name,
                country_name,
                unlocode,
                function,
                coordinates,
                is_failed_mapping
            ) 
        VALUES 
            (
                :port_name,
                :country_name,
                :unlocode,
                :function,
                :coordinates,
                :is_failed_mapping
            )
    """
    db.post_data(
        con=con,
        cursor=cur,
        query_str=query_save_port,
        data=ports
    )
        
    query_join_port_cargoline = """
        SELECT 
            c.id,
            c.unlocode,
            c.eta,
            c.etb,
            c.etd,
            c.quantity,
            c.material,
            p.function,
            p.port_name,
            p.country_name,
            p.function,
            p.coordinates,
            p.is_failed_mapping
        FROM 
            cargoline c 
        JOIN 
            port p 
        ON 
            p.unlocode = c.unlocode
        ORDER BY
            c.unlocode
    """

    results = db.get_data(cursor=cur, query_str=query_join_port_cargoline)
        
    list_success = []
    list_failed = []

    for result in results:
        dict_success = {}
        dict_failed = {}
        id = result[0]
        unlocode = result[1]
        eta = result[2]
        etb = result[3]
        etd = result[4]
        quantity = result[5]
        material = result[6]
        function = result[7]
        port_name = result[8]
        country_name = result[9]
        function = result[10]
        coordinates = result[11]
        is_failed_mapping = result[-1]

        if int(is_failed_mapping) == 0:
            dict_success = {
                'id':id,
                'unlocode':unlocode,
                'eta':eta,
                'etb':etb,
                'etd':etd,
                'quantity':quantity,
                'material':material,
                'port_function':function,
                'port_name':port_name,
                'country_name':country_name,
                'function':function,
                'coordinates':coordinates
            }
            list_success.append(dict_success)
        else:
            dict_failed = {
                'id':id,
                'unlocode':unlocode,
                'eta':eta,
                'etb':etb,
                'etd':etd,
                'quantity':quantity,
                'material':material,
                'port_function':function,
                'port_name':port_name,
                'country_name':country_name,
                'function':function,
                'coordinates':coordinates
            }
            list_failed.append(dict_failed)

    DB_PATH_PC = ROOT+'/db/port_cargoline.db'
    db_pc = Database()
    con_pc = db_pc.connect(db=DB_PATH_PC)
    cur_pc = con_pc.cursor()

    query_save_success = """
        INSERT INTO port_cargoline 
            (
                id,
                unlocode,
                eta,
                etb,
                etd,
                quantity,
                material,
                port_function,
                port_name,
                country_name,
                function,
                coordinates
            ) 
        VALUES 
            (
                :id,
                :unlocode,
                :eta,
                :etb,
                :etd,
                :quantity,
                :material,
                :port_function,
                :port_name,
                :country_name,
                :function,
                :coordinates
            )
    """
    db_pc.post_data(
        con=con_pc,
        cursor=cur_pc,
        query_str=query_save_success,
        data=list_success
    )

