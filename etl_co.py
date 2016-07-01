#!/usr/bin/python

import psycopg2
import database
from pymongo import MongoClient
from collections import defaultdict
import sys
from mongo_batch import MongoBatch

client = MongoClient("localhost:27017")
db = client.etlpro
orders = db.orders
batch = MongoBatch(orders, int(sys.argv[1])) if (len(sys.argv) > 1 and sys.argv[1].isdigit()) else MongoBatch(orders)

order_cnx = psycopg2.connect(database.dsn())
#order_cnx.time_zone = 'UTC'
order_cursor = order_cnx.cursor()

item_cnx = psycopg2.connect(database.dsn())
#item_cnx.time_zone = 'UTC'
item_cursor = item_cnx.cursor()

tracking_cnx = psycopg2.connect(database.dsn())
#tracking_cnx.time_zone = 'UTC'
tracking_cursor = tracking_cnx.cursor()

order_cursor.execute("""
    select id as order_id, first_name, last_name, shipping_address
    from orders
    order by order_id
""")

print "order query done"

item_cursor.execute("""
        select id as item_id, order_id, qty, description, price  from items
        order by order_id
    """)

print "item query done"

tracking_cursor.execute("""
    select order_id, status, tmstmp from tracking
    order by order_id
""")

print "tracking query done"

item_row = (0, 0, 0, "", 0)
tracking_row = (0, "", 0)

for (order_id, first_name, last_name, shipping_address) in order_cursor:
    items = []
    tracking = []
    doc = { "order_id" : order_id,
            "first_name" : first_name,
            "last_name" : last_name,
            "shipping_address" : shipping_address,
            "items" : items,
            "tracking" : tracking }
    
    while item_row != None and item_row[1] < order_id:
        print "item fetch"
        item_row = item_cursor.fetchone()

    while item_row != None and item_row[1] == order_id:
        items.append({ "item_id" : item_row[0],
                       "qty" : item_row[2],
                       "description" : item_row[3],
                       "price" : item_row[4] })
        item_row = item_cursor.fetchone()

    while tracking_row != None and tracking_row[0] < order_id:
        tracking_row = tracking_cursor.fetchone()

    while tracking_row != None and tracking_row[0] == order_id:
        tracking.append({ "status" : tracking_row[1],
                          "timestamp" : tracking_row[2] })
        tracking_row = tracking_cursor.fetchone()


    batch.add(doc)

batch.finalize()

order_cursor.close()
item_cursor.close()
tracking_cursor.close()
order_cnx.close()
item_cnx.close()
tracking_cnx.close()

client.close()
