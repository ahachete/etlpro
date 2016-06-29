CREATE TABLE orders (
	id			integer		PRIMARY KEY
,	first_name		text		
,	last_name		text		
,	shipping_address	text
);

CREATE TABLE items (
	id			integer
,	order_id		integer		NOT NULL REFERENCES orders(id)
,	qty			integer
,	description		text
,	price			integer
);

CREATE INDEX items_order_id ON items (order_id);

CREATE TABLE tracking (
	order_id		integer		NOT NULL REFERENCES orders(id)
,	status			text
,	tmstmp			timestamptz
);

CREATE INDEX tracking_order_id ON tracking (order_id);
