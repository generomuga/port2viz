from logging import error
from multiprocessing import Pool, TimeoutError
from bs4 import BeautifulSoup
from lib.database import Database

import time
import requests
import reverse_geocoder as rg
import sys
import pandas as pd

sys.setrecursionlimit(100000)

def _convert_lat_lon(coordinates):
    try:
        if len(coordinates) > 0:
            raw_lat, raw_lon = coordinates.split()
            lat = int(raw_lat[0:-1])/100
            lon = int(raw_lon[0:-1])/100
            return lat, lon
    except Exception as err:
        return "",""

def _get_formatted_addr(lat,lon):
    try:
        coordinates = (lat, lon)
        result = rg.search(coordinates)
        time.sleep(4)
        admin2 = result[0]['admin2']
        admin1 = result[0]['admin1']
        return admin1 if str(admin2) == '' else admin2
    except Exception as err:
        return err

def is_failed_mapping(**kwargs):
    function = str(kwargs['function']).replace(' ','')
    country = str(kwargs['country_name']).replace(' ','')
    port = str(kwargs['port_name']).replace(' ','')
    coordinates = str(kwargs['coordinates']).replace(' ','')

    try:
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
        return 1

def _get_data(rows, country_code, locode, country):
    # id = 0
    dict_port = {}
    for row in rows:
        if country_code+'  '+locode in str(row).strip():
            # id += 1
            cols = row.find_all("td")
            unlocode = country_code+'  '+locode
            country_name = country
            port_name = cols[2].string
            function = cols[5].string
            coordinates = cols[9].string
            lat, lon = _convert_lat_lon(coordinates)

            is_failed = is_failed_mapping(
                function=function,
                country_name=country_name,
                port_name=port_name,
                coordinates=coordinates
            )

            dict_port = {
                # "id": id,
                "port_name": port_name,
                "unlocode": unlocode,
                "country_name": country_name,
                "function": function,
                "coordinates": coordinates,
                "is_failed_mapping": is_failed,
                "lat": lat,
                "lon": lon,
            }

            return dict_port

def get_page_content(data):
    page = requests.get(data[0])
    
    # cd Projects\port2viz\env\Scripts\
    
    ports = []

    if page.status_code == 200:
        time.sleep(3)
        soup = BeautifulSoup(page.content, 'html5lib')
        rows = soup.find_all("tr")
        result = _get_data(rows, data[1], data[2], data[3])
        ports.append(result)

    print (ports)

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

    try:
        db_p = Database()
        con_p = db_p.connect(db='db/kdm_dev2.db')
        cur_p = con_p.cursor()

        cur_p.executemany(query_save_port, ports)
        con_p.commit()
        con_p.close()
    except Exception as err:
        print (err)

def save_to_xls(data, path):
    try:
        df = pd.DataFrame(data)
        df.to_excel(path)
    except Exception as err:
        print (err)

if __name__ == '__main__':

    db_p = Database()
    con_p = db_p.connect(db='db/kdm_dev2.db')
    cur_p = con_p.cursor()

    db_pc = Database()
    con_pc = db_pc.connect(db='db/kdm_port_to_viz_try.db')
    cur_pc = con_pc.cursor()

    # Reset port table
    db_p.truncate_table(
        con=con_p,
        cursor=cur_p,
        table='port'
    )

    # Reset port table
    db_pc.truncate_table(
        con=con_pc,
        cursor=cur_pc,
        table='port_cargoline'
    )

    links = [
        ('https://service.unece.org/trade/locode/al.htm','AL','BUT','<country_name>'),
        ('https://service.unece.org/trade/locode/al.htm','AL','BUT','<country_name>'),
        ('https://service.unece.org/trade/locode/al.htm','AL','HMR','<country_name>'),
        ('https://service.unece.org/trade/locode/al.htm','AL','HMR','<country_name>'),
        ('https://service.unece.org/trade/locode/al.htm','AL','SAR','<country_name>'),
        ('https://service.unece.org/trade/locode/al.htm','AL','SAR','<country_name>'),
        ('https://service.unece.org/trade/locode/al.htm','AL','SHG','<country_name>'),
        ('https://service.unece.org/trade/locode/al.htm','AL','SHG','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','AGS','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','AGS','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','ALH','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','ALH','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','ARB','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','ARB','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','ARE','<country_name>'),
        # ('https://service.unece.org/trade/locode/br.htm','BR','ARE','<country_name>'),
        # ('https://service.unece.org/trade/locode/br.htm','BR','BUN','<country_name>'),
        # ('https://service.unece.org/trade/locode/br.htm','BR','BUN','<country_name>'),
        # ('https://service.unece.org/trade/locode/br.htm','BR','RCH','<country_name>'),
        # ('https://service.unece.org/trade/locode/br.htm','BR','RCH','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','KDC','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','KDC','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','KLK','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','KLK','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','PIK','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','PIK','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','SNM','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','SNM','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','VAW','<country_name>'),
        # ('https://service.unece.org/trade/locode/by.htm','BY','VAW','<country_name>'),
        # ('https://service.unece.org/trade/locode/cr.htm','CR','CAB','<country_name>'),
        # ('https://service.unece.org/trade/locode/cr.htm','CR','CAB','<country_name>'),
        # ('https://service.unece.org/trade/locode/de.htm','DE','ACX','<country_name>'),
        # ('https://service.unece.org/trade/locode/de.htm','DE','ACX','<country_name>'),
        # ('https://service.unece.org/trade/locode/de.htm','DE','AMR','<country_name>'),
        # ('https://service.unece.org/trade/locode/de.htm','DE','AMR','<country_name>'),
        # ('https://service.unece.org/trade/locode/eh.htm','EH','EAI','<country_name>'),
        # ('https://service.unece.org/trade/locode/eh.htm','EH','EAI','<country_name>'),
        # ('https://service.unece.org/trade/locode/ga.htm','GA','NYA','<country_name>'),
        # ('https://service.unece.org/trade/locode/ga.htm','GA','NYA','<country_name>'),
        # ('https://service.unece.org/trade/locode/hr.htm','HR','CRS','<country_name>'),
        # ('https://service.unece.org/trade/locode/hr.htm','HR','CRS','<country_name>'),
        # ('https://service.unece.org/trade/locode/hr.htm','HR','DRK','<country_name>'),
        # ('https://service.unece.org/trade/locode/hr.htm','HR','DRK','<country_name>'),
        # ('https://service.unece.org/trade/locode/il.htm','IL','BGV','<country_name>'),
        # ('https://service.unece.org/trade/locode/il.htm','IL','BGV','<country_name>'),
        # ('https://service.unece.org/trade/locode/jp.htm','JP','AMX','<country_name>'),
        # ('https://service.unece.org/trade/locode/jp.htm','JP','AMX','<country_name>'),
        # ('https://service.unece.org/trade/locode/jp.htm','JP','ARJ','<country_name>'),
        # ('https://service.unece.org/trade/locode/jp.htm','JP','ARJ','<country_name>'),
        # ('https://service.unece.org/trade/locode/jp.htm','JP','CHG','<country_name>'),
        # ('https://service.unece.org/trade/locode/jp.htm','JP','CHG','<country_name>'),
        # ('https://service.unece.org/trade/locode/jp.htm','JP','CHW','<country_name>'),
        # ('https://service.unece.org/trade/locode/jp.htm','JP','CHW','<country_name>'),
        # ('https://service.unece.org/trade/locode/lk.htm','LK','HBA','<country_name>'),
        # ('https://service.unece.org/trade/locode/lk.htm','LK','HBA','<country_name>'),
        # ('https://service.unece.org/trade/locode/lk.htm','LK','KCT','<country_name>'),
        # ('https://service.unece.org/trade/locode/lk.htm','LK','KCT','<country_name>'),
        # ('https://service.unece.org/trade/locode/lk.htm','LK','OLU','<country_name>'),
        # ('https://service.unece.org/trade/locode/lk.htm','LK','OLU','<country_name>'),
        # ('https://service.unece.org/trade/locode/ma.htm','MA','BRE','<country_name>'),
        # ('https://service.unece.org/trade/locode/ma.htm','MA','BRE','<country_name>'),
        # ('https://service.unece.org/trade/locode/ma.htm','MA','EUN','<country_name>'),
        # ('https://service.unece.org/trade/locode/ma.htm','MA','EUN','<country_name>'),
        # ('https://service.unece.org/trade/locode/ma.htm','MA','TNG','<country_name>'),
        # ('https://service.unece.org/trade/locode/ma.htm','MA','TNG','<country_name>'),
        # ('https://service.unece.org/trade/locode/md.htm','MD','GIU','<country_name>'),
        # ('https://service.unece.org/trade/locode/md.htm','MD','GIU','<country_name>'),
        # ('https://service.unece.org/trade/locode/md.htm','MD','XXX','<country_name>'),
        # ('https://service.unece.org/trade/locode/md.htm','MD','XXX','<country_name>'),
        # ('https://service.unece.org/trade/locode/na.htm','NA','LUD','<country_name>'),
        # ('https://service.unece.org/trade/locode/na.htm','NA','LUD','<country_name>'),
        # ('https://service.unece.org/trade/locode/na.htm','NA','RUA','<country_name>'),
        # ('https://service.unece.org/trade/locode/na.htm','NA','RUA','<country_name>'),
        # ('https://service.unece.org/trade/locode/na.htm','NA','TSB','<country_name>'),
        # ('https://service.unece.org/trade/locode/na.htm','NA','TSB','<country_name>'),
        # ('https://service.unece.org/trade/locode/nz.htm','NZ','ORR','<country_name>'),
        # ('https://service.unece.org/trade/locode/nz.htm','NZ','ORR','<country_name>'),
        # ('https://service.unece.org/trade/locode/nz.htm','NZ','ORW','<country_name>'),
        # ('https://service.unece.org/trade/locode/nz.htm','NZ','ORW','<country_name>'),
        # ('https://service.unece.org/trade/locode/ph.htm','PH','ARA','<country_name>'),
        # ('https://service.unece.org/trade/locode/ph.htm','PH','ARA','<country_name>'),
        # ('https://service.unece.org/trade/locode/ph.htm','PH','BUG','<country_name>'),
        # ('https://service.unece.org/trade/locode/ph.htm','PH','BUG','<country_name>'),
        # ('https://service.unece.org/trade/locode/pk.htm','PK','BQM','<country_name>'),
        # ('https://service.unece.org/trade/locode/pk.htm','PK','BQM','<country_name>'),
        # ('https://service.unece.org/trade/locode/pk.htm','PK','GWD','<country_name>'),
        # ('https://service.unece.org/trade/locode/pk.htm','PK','GWD','<country_name>'),
        # ('https://service.unece.org/trade/locode/pk.htm','PK','KIA','<country_name>'),
        # ('https://service.unece.org/trade/locode/pk.htm','PK','KIA','<country_name>'),
        # ('https://service.unece.org/trade/locode/ss.htm','SS','RUM','<country_name>'),
        # ('https://service.unece.org/trade/locode/ss.htm','SS','RUM','<country_name>'),
        # ('https://service.unece.org/trade/locode/to.htm','TO','TBU','<country_name>'),
        # ('https://service.unece.org/trade/locode/to.htm','TO','TBU','<country_name>'),
        # ('https://service.unece.org/trade/locode/ug.htm','UG','KAB','<country_name>'),
        # ('https://service.unece.org/trade/locode/ug.htm','UG','KAB','<country_name>'),
        # ('https://service.unece.org/trade/locode/vi.htm','VI','PAX','<country_name>'),
        # ('https://service.unece.org/trade/locode/vi.htm','VI','PAX','<country_name>'),
        # ('https://service.unece.org/trade/locode/yt.htm','YT','MAM','<country_name>'),
        # ('https://service.unece.org/trade/locode/yt.htm','YT','MAM','<country_name>'),
    ]
    
    with Pool(processes=12) as pool:
        result = pool.map(get_page_content, links) # tuple of args for foo
        # print (async_result)

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
            p.is_failed_mapping,
            p.formatted_address
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
        is_failed_mapping = result[12]
        formatted_address = result[-1]

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
                'coordinates':coordinates,
                'formatted_address':formatted_address
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
                coordinates,
                formatted_address
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
                :coordinates,
                :formatted_address
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

    exp_path = 'export/test.xlsx'
    # Save failed mapping to xls
    save_to_xls(data, exp_path)
    
    # Display summary
    print ('MAPPING SUMMARY: successful:'+str(len(list_success))+ ' --- '+'failed:'+str(len(list_failed_id)))
    print ("Failed mapping file: "+exp_path)