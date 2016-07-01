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

cnx = psycopg2.connect(database.dsn())
#cnx.time_zone = 'UTC'
cursor = cnx.cursor()

items = defaultdict(list)
cursor.execute("""
        select id as item_id, order_id, qty, description, price  from items
    """)
prevlen = 0
for (item_id, order_id, qty, description, price) in cursor:
    items[order_id].append({ "item_id" : item_id,
                             "qty" : qty,
                             "description" : description,
                             "price" : price })
    if len(items) % 10000 == 0:
        if prevlen != len(items):
            print len(items)
            prevlen = len(items)

print "items loaded"

tracking = defaultdict(list)
cursor.execute("""
        select order_id, status, tmstmp from tracking
    """ )
prevlen = 0
for (order_id, status, time_stamp) in cursor:
    tracking[order_id].append({ "status" : status,
                                "timestamp" : time_stamp })
    if len(tracking) % 10000 == 0:
        if prevlen != len(tracking):
            print len(tracking)
            prevlen = len(tracking)


print "tracking loaded"


cursor.execute("""
  select id as order_id, first_name, last_name, shipping_address from orders
""")

for (order_id, first_name, last_name, shipping_address) in cursor:
    doc = { "order_id" : order_id,
            "first_name" : first_name,
            "last_name" : last_name,
            "shipping_address" : shipping_address,
            "items" : items[order_id],
            "tracking" : tracking[order_id] }
    batch.add(doc)

batch.finalize()

cursor.close()
cnx.close()

client.close()
