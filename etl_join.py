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
cursor = cnx.cursor()

join_query = """
SELECT		o.*, i.items, t.trackings
FROM		orders AS o
		INNER JOIN (
			SELECT		order_id, array_agg(ARRAY[id, qty, description, price]::text[]) AS items
			FROM		items
			GROUP BY	order_id
		) AS i
		ON (i.order_id = o.id)
		INNER JOIN (
			SELECT		order_id, array_agg(ARRAY[status, tmstmp]::text[]) AS trackings
			FROM		tracking
			GROUP BY	order_id
		) AS t
		ON (t.order_id = o.id)
"""
cursor.execute(join_query)

for (order_id, first_name, last_name, shipping_address, items, trackings) in cursor:
    item_list = []
    tracking_list = []
    doc = { "order_id" : order_id,
            "first_name" : first_name,
            "last_name" : last_name,
            "shipping_address" : shipping_address,
            "items" : item_list,
            "tracking" : tracking_list }

    for (item) in items:
	 item_list.append( { "item_id" : item[0],
                            "qty" : item[1],
                            "description" : item[2],
                            "price" : item[3] } )

    for (tracking) in trackings:
        tracking_list.append( { "status" : tracking[0],
                                "timestamp" : tracking[1] } )

    batch.add(doc)
batch.finalize()

cursor.close()
cnx.close()

client.close()
