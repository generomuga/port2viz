from lib.database import Database
from lib.scraper import Scraper

import os
import pandas as pd
import numpy as np
import configparser as cp
import logging

import random

def convert_to_list(results):
    list = []
    try:
        list.append([result for result in results])
        logging.info('Converting data to list...')
        return list
    except Exception as err:
        logging.error(err)
        print (err)    
   
def set_locode_url(**kwargs):
    try:
        base_url = kwargs['base_url']
        country_code = kwargs['country_code']
        extension = kwargs['extension']
        logging.info('Setting locode url...')
        return base_url+country_code+extension
    except Exception as err:
        logging.error(err)
        print (err)

def get_distinct_location(locations):
    try:
        logging.info('Getting unique location ...')
        return np.unique(locations)
    except Exception as err:
        logging.error(err)
        print (err)

def get_country_name(content, country_code):
    rows = content.find_all("tr")
    try:
        for row in rows:
            if country_code in str(row).strip():
                cols = row.find_all("td")
                country_name = cols[1].string
                logging.info('Getting country name...')
                return country_name
    except Exception as err:
        logging.error(err)
        print (err)

def is_failed_mapping(**kwargs):
    function = kwargs['function']
    country = kwargs['country_name']
    port = kwargs['port_name']
    coordinates = kwargs['coordinates']

    try:
        logging.info('Getting country name...')
        if '1' not in function:
            return 1
        if len(country) <= 0 or str(country) == '':
            return 1
        if len(port) <= 0 or str(port) == '':
            return 1
        if len(coordinates) <= 0 or str(coordinates) == '':
            return 1
        return 0
    except Exception as err:
        logging.error(err)
        print (err)

def save_to_xls(data, path):
    try:
        df = pd.DataFrame(data)
        df.to_excel(path)
        logging.info('Saving to excel...')
    except Exception as err:
        logging.error(err)
        print (err)

if __name__ == '__main__':

    # Get root path
    ROOT = os.path.abspath(os.curdir)

    # Initialize config reader and parser
    CONFIG_PATH = ROOT+'/config/conf.ini'
    config = cp.ConfigParser()
    config.read(CONFIG_PATH)

    # Initialize logging settings
    LOG_PATH = ROOT+config['PATH']['LOG_PATH']
    logging.basicConfig(filename=LOG_PATH,level=logging.DEBUG, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    # Set required paths and attributes
    DB_PATH_P = ROOT+config['PATH']['DB_KDM']
    DB_PATH_PC = ROOT+config['PATH']['DB_KDM_P2V']
    BASE_URL = config['PATH']['BASE_URL']
    BASE_URL_LOCODE = config['PATH']['BASE_URL_LOCODE']
    EXTENSION = config['PATH']['EXTENSION']
    EXPORT_PATH = ROOT+config['PATH']['EXPORT_PATH']

    # Initialize db class and components for port db
    db_p = Database()
    con_p = db_p.connect(db=DB_PATH_P)
    cur_p = con_p.cursor()
    
    # Initialize db class and components for kdm_vis_to_port db
    db_pc = Database()
    con_pc = db_pc.connect(db=DB_PATH_PC)
    cur_pc = con_pc.cursor()

    # Reset port table
    db_p.truncate_table(
        con=con_p,
        cursor=cur_p,
        table='port'
    )

    # Reset port_cargoline table
    db_pc.truncate_table(
        con=con_pc,
        cursor=cur_pc,
        table='port_cargoline'
    )

    # Initialize scraper class
    scp = Scraper()

    # Get locode data
    query_get_unlocode = "SELECT unlocode FROM cargoline ORDER BY unlocode;"
    results = db_p.get_data(cursor=cur_p, query_str=query_get_unlocode)

    # Convert the db results to a list and get the distinct values
    list_location = convert_to_list(results)
    locinfos = get_distinct_location(list_location)
    count_locinfos = len(locinfos)
    print ('+ Found '+str(count_locinfos)+' unique unlocodes to be scraped...')

    # Initialize variable that will handle port data and id
    ports = []
    id = 0

    # Get home page content
    home_page_content = scp.get_page_content(
        url=BASE_URL
    )

    scp_ctr = 0
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
        scp_ctr += 1
        print ('+ Scraping unlocode '+str(country_code)+' '+str(locode)+' process '+str(scp_ctr)+'/'+str(count_locinfos)+'...')
        
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
                # Activate this during the actual run
                # is_failed = is_failed_mapping(
                #     function=function,
                #     country_name=country_name,
                #     port_name=port_name,
                #     coordinates=coordinates
                # )
                # Activate this if you want to test the function
                is_failed = random.randrange(0,2)

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
                if len(ports) > 0:
                    print ('--+ Data has been successfully scraped!')
    
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
    db_p.post_data(
        con=con_p,
        cursor=cur_p,
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

    results = db_p.get_data(
        cursor=cur_p, 
        query_str=query_join_port_cargoline
    )
    
    # Initialize list for the data frames
    list_failed_id = []
    list_failed_unlocode = []
    list_failed_eta = []
    list_failed_etb = []
    list_failed_etd = []
    list_failed_quantity = []
    list_failed_material = []
    list_failed_function = []
    list_failed_port_name = []
    list_failed_country_name = []
    list_failed_coordinates = []

    # Initiliaze list to handle dict_success
    list_success = []
    
    for result in results:
        # Intialize dictionary for success mapping
        dict_success = {}

        # Parse result
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

        # Classiy successful and failed mapping
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
            list_failed_id.append(id)
            list_failed_unlocode.append(unlocode)
            list_failed_eta.append(eta)
            list_failed_etb.append(etb)
            list_failed_etd.append(etd)
            list_failed_quantity.append(quantity)
            list_failed_material.append(material)
            list_failed_function.append(function)
            list_failed_port_name.append(port_name)
            list_failed_country_name.append(country_name)
            list_failed_coordinates.append(coordinates)

    # Query to save successful mapping
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

    # Save data to port_cargoline table
    db_pc.post_data(
        con=con_pc,
        cursor=cur_pc,
        query_str=query_save_success,
        data=list_success
    )

    # Set failed mapping to dictionary
    data = {
        "id": list_failed_id,
        "unlocode": list_failed_unlocode,
        "eta": list_failed_eta,
        "etb": list_failed_etb,
        "etd": list_failed_etd,
        "quantity": list_failed_quantity,
        "material": list_failed_material,
        "port_function": list_failed_function,
        "port_name": list_failed_port_name,
        "country_name": list_failed_country_name,
        "function": list_failed_function,
        "coordinates": list_failed_coordinates
    }

    print ('MAPPING SUMMARY: successful:'+str(len(list_success))+ ' --- '+'failed:'+str(len(list_failed_id)))

    # Save failed mapping to xls
    save_to_xls(data, EXPORT_PATH)
    print ("Failed mapping file: "+EXPORT_PATH)