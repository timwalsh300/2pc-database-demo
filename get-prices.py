import sys
import mysql.connector
import requests
import json

cnx = mysql.connector.connect(user='root',
                              password=sys.argv[1],
                              database='yelp_db')
cursor = cnx.cursor()

statement1 = '''CREATE TABLE prices
            ( id INT NOT NULL AUTO_INCREMENT,
            business_id VARCHAR(22) NOT NULL,
            price VARCHAR(255) NULL,
            PRIMARY KEY (id)) ENGINE = InnoDB'''
cursor.execute(statement1)

query = ('''SELECT id FROM business WHERE id IN
          (SELECT business_id FROM category WHERE
          category = %s) AND city = "Las Vegas"''')

statement2 = ('''INSERT INTO prices (business_id, price)
          VALUES (%s, %s)''')

cuisines = ['Korean', 'Mexican', 'Donuts', 'Italian',
            'Bakeries', 'Thai', 'Pizza', 'Seafood',
            'Sandwiches', 'Burgers', 'Barbeque']

url_prefix = 'https://api.yelp.com/v3/businesses/'
api_key = 'Insert-YourOwnKeyForTheYelpAPIHere'
auth = {'Authorization': 'Bearer {}'.format(api_key)}
current = ' '
price_map = {}
for cs in cuisines:
    if current != cs:
        print('getting {}'.format(cs))
        current = cs
    cursor.execute(query, (cs,))
    for (id,) in cursor:
        # handle cases where business is in two categories
        if id in price_map:
            continue
        # call Yelp Fusion to get price data
        url = url_prefix + id
        response = requests.request('GET', url, headers=auth)
        try:
            price_str = response.json()['price']
        except:
            continue
        # store price data in my table
        price_val = 2
        if price_str == '$':
            price_val = 1
        elif price_str == '$$':
            price_val = 2
        elif price_str == '$$$':
            price_val = 3
        elif price_str == '$$$$':
            price_val = 4
        price_map[id] = price_val
for k, v in price_map.items():
    cursor.execute(statement2, (k, v))
cnx.commit()
cnx.close()
