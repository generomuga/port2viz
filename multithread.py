from multiprocessing import Pool, TimeoutError
import time
from bs4 import BeautifulSoup
import requests
import reverse_geocoder as rg

import sys
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
    id = 0
    dict_port = {}
    for row in rows:
        if country_code+'  '+locode in str(row).strip():
            id += 1
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
                "id": id,
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
    return 1

if __name__ == '__main__':

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
        ('https://service.unece.org/trade/locode/br.htm','BR','ARE','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','BUN','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','BUN','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','RCH','<country_name>'),
        ('https://service.unece.org/trade/locode/br.htm','BR','RCH','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','KDC','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','KDC','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','KLK','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','KLK','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','PIK','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','PIK','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','SNM','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','SNM','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','VAW','<country_name>'),
        ('https://service.unece.org/trade/locode/by.htm','BY','VAW','<country_name>'),
        ('https://service.unece.org/trade/locode/cr.htm','CR','CAB','<country_name>'),
        ('https://service.unece.org/trade/locode/cr.htm','CR','CAB','<country_name>'),
        ('https://service.unece.org/trade/locode/de.htm','DE','ACX','<country_name>'),
        ('https://service.unece.org/trade/locode/de.htm','DE','ACX','<country_name>'),
        ('https://service.unece.org/trade/locode/de.htm','DE','AMR','<country_name>'),
        ('https://service.unece.org/trade/locode/de.htm','DE','AMR','<country_name>'),
        ('https://service.unece.org/trade/locode/eh.htm','EH','EAI','<country_name>'),
        ('https://service.unece.org/trade/locode/eh.htm','EH','EAI','<country_name>'),
        ('https://service.unece.org/trade/locode/ga.htm','GA','NYA','<country_name>'),
        ('https://service.unece.org/trade/locode/ga.htm','GA','NYA','<country_name>'),
        ('https://service.unece.org/trade/locode/hr.htm','HR','CRS','<country_name>'),
        ('https://service.unece.org/trade/locode/hr.htm','HR','CRS','<country_name>'),
        ('https://service.unece.org/trade/locode/hr.htm','HR','DRK','<country_name>'),
        ('https://service.unece.org/trade/locode/hr.htm','HR','DRK','<country_name>'),
        ('https://service.unece.org/trade/locode/il.htm','IL','BGV','<country_name>'),
        ('https://service.unece.org/trade/locode/il.htm','IL','BGV','<country_name>'),
        ('https://service.unece.org/trade/locode/jp.htm','JP','AMX','<country_name>'),
        ('https://service.unece.org/trade/locode/jp.htm','JP','AMX','<country_name>'),
        ('https://service.unece.org/trade/locode/jp.htm','JP','ARJ','<country_name>'),
        ('https://service.unece.org/trade/locode/jp.htm','JP','ARJ','<country_name>'),
        ('https://service.unece.org/trade/locode/jp.htm','JP','CHG','<country_name>'),
        ('https://service.unece.org/trade/locode/jp.htm','JP','CHG','<country_name>'),
        ('https://service.unece.org/trade/locode/jp.htm','JP','CHW','<country_name>'),
        ('https://service.unece.org/trade/locode/jp.htm','JP','CHW','<country_name>'),
        ('https://service.unece.org/trade/locode/lk.htm','LK','HBA','<country_name>'),
        ('https://service.unece.org/trade/locode/lk.htm','LK','HBA','<country_name>'),
        ('https://service.unece.org/trade/locode/lk.htm','LK','KCT','<country_name>'),
        ('https://service.unece.org/trade/locode/lk.htm','LK','KCT','<country_name>'),
        ('https://service.unece.org/trade/locode/lk.htm','LK','OLU','<country_name>'),
        ('https://service.unece.org/trade/locode/lk.htm','LK','OLU','<country_name>'),
        ('https://service.unece.org/trade/locode/ma.htm','MA','BRE','<country_name>'),
        ('https://service.unece.org/trade/locode/ma.htm','MA','BRE','<country_name>'),
        ('https://service.unece.org/trade/locode/ma.htm','MA','EUN','<country_name>'),
        ('https://service.unece.org/trade/locode/ma.htm','MA','EUN','<country_name>'),
        ('https://service.unece.org/trade/locode/ma.htm','MA','TNG','<country_name>'),
        ('https://service.unece.org/trade/locode/ma.htm','MA','TNG','<country_name>'),
        ('https://service.unece.org/trade/locode/md.htm','MD','GIU','<country_name>'),
        ('https://service.unece.org/trade/locode/md.htm','MD','GIU','<country_name>'),
        ('https://service.unece.org/trade/locode/md.htm','MD','XXX','<country_name>'),
        ('https://service.unece.org/trade/locode/md.htm','MD','XXX','<country_name>'),
        ('https://service.unece.org/trade/locode/na.htm','NA','LUD','<country_name>'),
        ('https://service.unece.org/trade/locode/na.htm','NA','LUD','<country_name>'),
        ('https://service.unece.org/trade/locode/na.htm','NA','RUA','<country_name>'),
        ('https://service.unece.org/trade/locode/na.htm','NA','RUA','<country_name>'),
        ('https://service.unece.org/trade/locode/na.htm','NA','TSB','<country_name>'),
        ('https://service.unece.org/trade/locode/na.htm','NA','TSB','<country_name>'),
        ('https://service.unece.org/trade/locode/nz.htm','NZ','ORR','<country_name>'),
        ('https://service.unece.org/trade/locode/nz.htm','NZ','ORR','<country_name>'),
        ('https://service.unece.org/trade/locode/nz.htm','NZ','ORW','<country_name>'),
        ('https://service.unece.org/trade/locode/nz.htm','NZ','ORW','<country_name>'),
        ('https://service.unece.org/trade/locode/ph.htm','PH','ARA','<country_name>'),
        ('https://service.unece.org/trade/locode/ph.htm','PH','ARA','<country_name>'),
        ('https://service.unece.org/trade/locode/ph.htm','PH','BUG','<country_name>'),
        ('https://service.unece.org/trade/locode/ph.htm','PH','BUG','<country_name>'),
        ('https://service.unece.org/trade/locode/pk.htm','PK','BQM','<country_name>'),
        ('https://service.unece.org/trade/locode/pk.htm','PK','BQM','<country_name>'),
        ('https://service.unece.org/trade/locode/pk.htm','PK','GWD','<country_name>'),
        ('https://service.unece.org/trade/locode/pk.htm','PK','GWD','<country_name>'),
        ('https://service.unece.org/trade/locode/pk.htm','PK','KIA','<country_name>'),
        ('https://service.unece.org/trade/locode/pk.htm','PK','KIA','<country_name>'),
        ('https://service.unece.org/trade/locode/ss.htm','SS','RUM','<country_name>'),
        ('https://service.unece.org/trade/locode/ss.htm','SS','RUM','<country_name>'),
        ('https://service.unece.org/trade/locode/to.htm','TO','TBU','<country_name>'),
        ('https://service.unece.org/trade/locode/to.htm','TO','TBU','<country_name>'),
        ('https://service.unece.org/trade/locode/ug.htm','UG','KAB','<country_name>'),
        ('https://service.unece.org/trade/locode/ug.htm','UG','KAB','<country_name>'),
        ('https://service.unece.org/trade/locode/vi.htm','VI','PAX','<country_name>'),
        ('https://service.unece.org/trade/locode/vi.htm','VI','PAX','<country_name>'),
        ('https://service.unece.org/trade/locode/yt.htm','YT','MAM','<country_name>'),
        ('https://service.unece.org/trade/locode/yt.htm','YT','MAM','<country_name>'),
    ]
    
    with Pool(processes=10) as pool:
        async_result = pool.map(get_page_content, links) # tuple of args for foo
        print (async_result)