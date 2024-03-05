import pyap
import requests
from bs4 import BeautifulSoup
import re
import psycopg2


file = open(f'{__name__}_errors.txt', 'w')

session = requests.Session()
session.proxies = {
    'http': "http://spybuuiosy:Mlil9nOQ56Wnfr6ukb@gate.smartproxy.com:10000",
    'https': "https://spybuuiosy:Mlil9nOQ56Wnfr6ukb@gate.smartproxy.com:10000"
}


def _extract_compass_listings(address):
    url = "https://www.compass.com/api/v3/omnisuggest/autocomplete"
    query = {
        "q": address,
        "listingTypes": [1]
    }
    response = session.post(
        url,
        json=query
    )
    return response.json().get('categories')


def _get_compass_buildings(address: str):
    res = pyap.parse(address, country="US")

    if not len(res):
        file.write(address + '\n')
        return

    line = res[0].as_dict().get('full_street')
    line = line.replace('STREET', 'st').replace('COURT', 'ct').replace('TERRACE', 'terr').replace(
        'DRIVE', 'dr').replace('BOULEVARD', 'blvd').replace('AVENUE', 'ave').replace('LANE', 'ln')

    if not line:
        file.write(address + '|' + line + '|' + '\n')
        return

    res = _extract_compass_listings(line)

    if not res:
        file.write(address + '|' + line + '|' + '\n')
        return

    return res


def get_compass_building_info(address: str):
    res = _get_compass_buildings(address)
    if res:
        #print(address)
        redirect = res[0].get('items')[0].get('redirectUrl')
        url = f"https://compass.com{redirect}"
        #print(url)
        source = session.get(url).text

        bs = BeautifulSoup(source, features='html.parser')
        try:
            
        
            year_built = bs.find(
                'li', {'data-tn': 'building-page-summary-building-age'}).text.replace('Built in ', '')
        except Exception:
            year_built = ""
        try:
            stories_and_units = bs.find(
               'li', {'data-tn': 'building-page-summary-units-floors'}).text
        except Exception: 
            stories_and_units = "" 
    
        stories = re.search(r'([0-9]+)\sStories',
                            stories_and_units)
        if stories:
            stories = stories.group(1)

        units = re.search(r'([0-9]+)\sUnits',
                          stories_and_units)
        if units:
            units = units.group(1)

        correct_address = bs.find(
            'div', {'data-tn': 'building-page-summary-address'}).get_text('')
        photo_link = bs.find (
            "img",{'id': 'media-gallery-hero-image'
            }
        ).get('src')
        street_address= correct_address.split(",")[0].upper()
        
        return year_built, stories, units, correct_address, photo_link, street_address
    
    #script insert into table

if __name__ == "__main__":
    connection= psycopg2.connect(
        host="127.0.0.1", 
        #database="prospector",
        #user="Jennifer",
        database= "raven_prospector",
        user= "postgres",
        password="Bigbear5k$",
        port=5432
    )  
    cursor = connection.cursor()
    cursor.execute("SELECT id, associationaddress FROM properties")
    for uid, address in cursor.fetchall():
        print(address)
        try:

            extracted_info = get_compass_building_info(address)
            print(extracted_info)
            if extracted_info:
                cursor.execute('INSERT INTO properties_realtor_params(property_id, year_built, stories, units, correct_address, photo_link, street_address) VALUES (%s,%s, %s, %s, %s, %s,%s);',[uid,*extracted_info])
                connection.commit()
        except: print('problem with proxy')