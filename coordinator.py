import tkinter as tk
import io
from PIL import Image, ImageTk
from urllib.request import urlopen
import sys
import urllib.request
import xml.etree.ElementTree as ET
import mysql.connector
import heapq
import hashlib
import socket
import  _thread
import re

def second_phase(command):
    for r in replicas:
        phase_complete = False
        while not phase_complete:
            try:
                print('sending {} to {}'.format(command, r))
                sock.sendto(command.encode('ASCII'), (r, port))
                (data, address)  = sock.recvfrom(3)
                print('remote node {} acknowledges: {}'
                      .format(r, data.decode()))
                phase_complete = True
            except:
                # error sending or recv timed-out
                # so keep trying while client recovers
                continue

def two_phase_commit(statements):
    s = ';'
    joined_statement = s.join(statements)
    # send statement to prepare
    for r in replicas:
        phase_complete = False
        votes = []
        while not phase_complete:
            try:
                print('sending {} statements to prepare'.format(r))
                sock.sendto(joined_statement.encode('ASCII'), (r, port))
                # get vote
                (vote, address) = sock.recvfrom(3)
                if re.search('[a-zA-Z]', vote.decode()) is None:
                    continue
                phase_complete = True
            except:
                # error sending or recv timed-out
                # so keep trying while client recovers
                continue
        votes.append(vote.decode())
        print('received vote from {}: {}'.format(r, vote.decode()))
    if 'nay' in votes:
        # abort
        second_phase('abo')
        return False
    else:
        # commit
        second_phase('com')
        return True

# generate the map image to display
def generate_map(list = []):
    prefix = 'https://maps.googleapis.com/maps/api/staticmap?center=las+vegas,+nv&zoom=12&size=800x800&maptype=roadmap'
    auth = 'InsertYourOwnKeyForTheGoogleMapsAPIhere'
    markers = ''
    for marker in list:
        markers += marker
    map_url = prefix + markers + auth
    image_bytes = urlopen(map_url).read()
    data_stream = io.BytesIO(image_bytes)
    map_image = Image.open(data_stream)
    return ImageTk.PhotoImage(map_image)

# convert a price number to a dollar sign string
def price_to_dollar_signs(price):
    if price == '1':
        return '$'
    elif price == '2':
        return '$$'
    elif price == '3':
        return '$$$'
    elif price == '4':
        return '$$$$'
    else:
        return '?'

# convert dollar sign string to price float
def dollar_signs_to_price(signs):
    if signs == '$':
        return 1.0
    elif signs == '$$':
        return 2.0
    elif signs == '$$$':
        return 3.0
    elif signs == '$$$$':
        return 4.0
    else:
        return -1.0

# find the range of a given column in the business table for
# entries in Las Vegas so we can use it to normalize distance
def get_range(cursor, column):
    query = ('''SELECT {}({}) AS {} FROM business
             WHERE city = "Las Vegas"'''
             .format('MAX', column, column))
    cursor.execute(query)
    for (x,) in cursor:
        max = float(x)
    query = ('''SELECT {}({}) AS {} FROM business
             WHERE city = "Las Vegas"'''
             .format('MIN', column, column))
    cursor.execute(query)
    for (x,) in cursor:
        min = float(x)
    return (max - min)

# find the latitude and longitude of a given address in Las Vegas
# using the Google API
def get_target_latlong(address):
    preface = 'https://maps.googleapis.com/maps/api/geocode/xml?address='
    middle = address.replace(' ', '+')
    suffix = ',+las+vegas,+nevada&key=InsertYourOwnKeyForTheGoogleMapsAPIhere'
    with urllib.request.urlopen(preface + middle + suffix) as response:
       xml = response.read()
    xml_root = ET.fromstring(xml).find('result').find('geometry')
    q_lat = float(xml_root.find('location').find('lat').text)
    q_long = float(xml_root.find('location').find('lng').text)
    return (q_lat, q_long)

# insert new restaurant into the database and its replica
def insert(q_name, q_cuisine, q_location, q_stars, q_price,
           frame, map_label, result_text, result_label):

    price_val = int(dollar_signs_to_price(q_price))
    (q_lat, q_long) = get_target_latlong(q_location)

    id_input = q_name + q_location
    id = hashlib.md5(id_input.encode()).hexdigest()[:22]

    statement1 = ('INSERT INTO business (id, name, address, city, ' +
                  'state, latitude, longitude, stars) VALUES ' +
                  '("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")'
                  .format(id, q_name, q_location, 'Las Vegas', 'NV',
                          q_lat, q_long, q_stars))
    statement2 = ('INSERT INTO prices (business_id, price) ' +
                  'VALUES ("{}", "{}")'.format(id, price_val))
    statement3 = ('INSERT INTO category (business_id, category) ' +
                  'VALUES ("{}", "{}")'.format(id, q_cuisine))
    # try to make insertions locally first
    try:
        cursor.execute(statement1)
        cursor.execute(statement2)
        cursor.execute(statement3)
    except:
        cnx.rollback()
        all_results_string = '\nFailed to insert {}'.format(q_name)
        result_text.set(all_results_string)
        result_label.configure(text=result_text.get())
        return

    # now try 2PC across network
    if two_phase_commit([statement1, statement2, statement3]):
        # 2PC succeeded
        cnx.commit()
        marker_list = []
        marker_list.append('&markers=color:red%7Clabel:X%7C{},{}'.format(q_lat, q_long))
        frame.image = generate_map(marker_list)
        map_label.configure(image=frame.image)
        all_results_string = '\nSuccessfully inserted {}'.format(q_name)
        result_text.set(all_results_string)
        result_label.configure(text=result_text.get())
    else:
        # 2PC failed
        cnx.rollback()
        all_results_string = '\nFailed to insert {}'.format(q_name)
        result_text.set(all_results_string)
        result_label.configure(text=result_text.get())

# get results for the query and print them out
def query(q_cuisine, q_location, location_wt,
          q_stars, stars_wt, q_price, price_wt,
          frame, map_label, result_text, result_label):
    (q_lat, q_long) = get_target_latlong(q_location)
    marker_list = []
    marker_list.append('&markers=color:red%7Clabel:X%7C{},{}'.format(q_lat, q_long))
    latrange = get_range(cursor, 'latitude')
    longrange = get_range(cursor, 'longitude')
    starsrange = get_range(cursor, 'stars')
    query_all = '''SELECT business.id, business.latitude,
                business.longitude, business.stars, prices.price
                FROM business INNER JOIN prices ON business.id =
                prices.business_id WHERE business.id IN
                (SELECT business_id FROM category WHERE
                category = %s) AND business.city = "Las Vegas"'''
    cursor.execute(query_all, (q_cuisine,))
    heap = []
    for (id, lat, long, stars, price) in cursor:
        if (lat is None) or (long is None):
            continue
        dist = ((location_wt / 2) * abs((lat - q_lat)/latrange) +
           (location_wt / 2) * abs((long - q_long)/longrange) +
           stars_wt * abs((stars - q_stars)/starsrange) +
           price_wt * abs((float(price) - dollar_signs_to_price(q_price))/3))
        dist = dist * -1
        if len(heap) < 10:
            heapq.heappush(heap, (dist, id))
        elif heap[0][0] < dist:
            heapq.heappushpop(heap, (dist, id))
    query_results = '''SELECT business.name, business.address,
                 business.stars, business.latitude, business.longitude,
                 prices.price FROM business INNER JOIN prices
                 ON business.id = prices.business_id
                 WHERE business.id = %s'''
    print('Top results are...')
    top9 = heapq.nlargest(9, heap)
    rank = 1
    result_list = []
    for neighbor in top9:
        cursor.execute(query_results, (neighbor[1],))
        for (name, address, stars, lat, long, price) in cursor:
            dollar_signs = price_to_dollar_signs(price)
            result_string = ('{}, {}, {}: {}'
                             .format(stars, dollar_signs, name, address))
            result_list.append(str(rank) + ': ' + result_string)
            marker_list.append('&markers=color:yellow%7Clabel:{}%7C{},{}'.format(rank, lat, long))
            rank += 1
    frame.image = generate_map(marker_list)
    map_label.configure(image=frame.image)
    all_results_string = '\nTop Results...\n\n'
    for line in result_list:
        all_results_string += line + '\n'
    result_text.set(all_results_string)
    result_label.configure(text=result_text.get())

# build the GUI and get input
def user_interface():
    # build the GUI
    window = tk.Tk()
    window.title('Eat Las Vegas')

    frame = tk.Frame(window, width=1600, height=800)
    frame.pack()

    map_image_tk = generate_map()
    map_label = tk.Label(frame, image=map_image_tk)
    map_label.grid(row=0, column=3, rowspan=20)

    tk.Label(frame, text='Criteria').grid(row=0, column=1)
    tk.Label(frame, text='Weight').grid(row=0, column=2)

    tk.Label(frame, text='Address').grid(row=1, column=0)
    q_location = tk.Entry(frame, width=25)
    q_location.insert(0, 'Nellis Air Force Base')
    q_location.grid(row=1, column=1)
    location_wt = tk.Entry(frame, width=4)
    location_wt.insert(0, '0.8')
    location_wt.grid(row=1, column=2)

    tk.Label(frame, text='Cuisine').grid(row=2, column=0)
    cuisines = ['Korean', 'Mexican', 'Donuts', 'Italian',
                'Bakeries', 'Thai', 'Pizza', 'Seafood',
                'Sandwiches', 'Burgers', 'Barbeque']
    q_cuisine = tk.StringVar(frame)
    q_cuisine.set(cuisines[2])
    tk.OptionMenu(frame, q_cuisine, *cuisines).grid(row=2, column=1)

    tk.Label(frame, text='Stars').grid(row=3, column=0)
    star_values = [1, 2, 3, 4, 5]
    q_stars = tk.IntVar(frame)
    q_stars.set(star_values[4])
    tk.OptionMenu(frame, q_stars, *star_values).grid(row=3, column=1)
    stars_wt = tk.Entry(frame, width=4)
    stars_wt.insert(0, '0.1')
    stars_wt.grid(row=3, column=2)

    tk.Label(frame, text='Price').grid(row=4, column=0)
    price_values = ['$', '$$', '$$$', '$$$$']
    q_price = tk.StringVar(frame)
    q_price.set(price_values[0])
    tk.OptionMenu(frame, q_price, *price_values).grid(row=4, column=1)
    price_wt = tk.Entry(frame, width=4)
    price_wt.insert(0, '0.1')
    price_wt.grid(row=4, column=2)

    result_text = tk.StringVar(frame)
    result_label = tk.Label(frame, text=result_text.get(), justify=tk.LEFT)
    result_label.grid(row=8, column=0, columnspan=3)

    tk.Label(frame, text='Name (to insert)').grid(row=5, column=0)
    q_name = tk.Entry(frame, width=25)
    q_name.insert(0, 'Tim\'s Tiny Donuts')
    q_name.grid(row=5, column=1)

    query_button = tk.Button(frame, text='Search',
                             command= lambda: query(q_cuisine.get(),
                                                    q_location.get(),
                                                    float(location_wt.get()),
                                                    q_stars.get(),
                                                    float(stars_wt.get()),
                                                    q_price.get(),
                                                    float(price_wt.get()),
                                                    frame,
                                                    map_label,
                                                    result_text,
                                                    result_label))
    query_button.grid(row=6, column=1)

    insert_button = tk.Button(frame, text='Insert',
                             command= lambda: insert(q_name.get(),
                                                    q_cuisine.get(),
                                                    q_location.get(),
                                                    q_stars.get(),
                                                    q_price.get(),
                                                    frame,
                                                    map_label,
                                                    result_text,
                                                    result_label))
    insert_button.grid(row=7, column=1)

    # get input
    frame.mainloop()

# main entry point for the program
cnx = mysql.connector.connect(user='root',
                              password=sys.argv[1],
                              database='yelp_db')
cursor = cnx.cursor()
port = 9999
replicas = [sys.argv[2], sys.argv[3]]
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(5)
user_interface()
sock.close()
