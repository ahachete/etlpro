CREATE TABLE IF NOT EXISTS orders (
	id			integer		PRIMARY KEY
,	first_name		text		
,	last_name		text		
,	shipping_address	text
);

CREATE TABLE IF NOT EXISTS items (
	id			integer
,	order_id		integer		NOT NULL REFERENCES orders(id)
,	qty			integer
,	description		text
,	price			integer
);

CREATE INDEX IF NOT EXISTS items_order_id ON items (order_id);

CREATE TABLE IF NOT EXISTS tracking (
	order_id		integer		NOT NULL REFERENCES orders(id)
,	status			text
,	tmstmp			timestamptz
);

CREATE INDEX IF NOT EXISTS tracking_order_id ON tracking (order_id);

TRUNCATE items;
TRUNCATE tracking;
TRUNCATE orders CASCADE;
