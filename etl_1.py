#!/usr/bin/python

import psycopg2
from pymongo import MongoClient
import database
import sys
from mongo_batch import MongoBatch


client = MongoClient("localhost:27017")
db = client.etlpro
orders = db.orders
batch = MongoBatch(orders, int(sys.argv[1])) if (len(sys.argv) > 1 and sys.argv[1].isdigit()) else MongoBatch(orders)

cnx = psycopg2.connect(database.dsn())

#cnx.time_zone = 'UTC'
cursor = cnx.cursor()

item_cnx = psycopg2.connect(database.dsn())
#item_cnx.time_zone = 'UTC'
item_cursor = item_cnx.cursor()

tracking_cnx = psycopg2.connect(database.dsn())
#tracking_cnx.time_zone = 'UTC'
tracking_cursor = tracking_cnx.cursor()

order_query_stmt = ("select id as order_id, first_name, last_name, shipping_address "
                    "from orders")

cursor.execute(order_query_stmt)

for (order_id, first_name, last_name, shipping_address) in cursor:
    item_list = []
    tracking_list = []
    doc = { "order_id" : order_id,
            "first_name" : first_name,
            "last_name" : last_name,
            "shipping_address" : shipping_address,
            "items" : item_list,
            "tracking" : tracking_list }
    item_cursor.execute("""
        select id as item_id, order_id, qty, description, price 
        from items where order_id = %s
    """ % (order_id))
    for (item_id, order_id, qty, description, price) in item_cursor:
        item_list.append( { "item_id" : item_id,
                            "qty" : qty,
                            "description" : description,
                            "price" : price } )
    tracking_cursor.execute("""
        select order_id, status, tmstmp
        from tracking where order_id = %s
    """ % (order_id))
    for (order_id, status, time_stamp) in tracking_cursor:
        tracking_list.append( { "status" : status,
                                "timestamp" : time_stamp } )
    batch.add(doc)

cursor.close()
item_cursor.close()
tracking_cursor.close()
cnx.close()

batch.finalize()
client.close()
