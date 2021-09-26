CREATE TABLE IF NOT EXISTS port_cargoline (
	id varchar(255),
	unlocode varchar(64),
	eta timestamp,
	etb timestamp,
	etd timestamp,
	quantity real,
	material varchar(128),
	port_function varchar(64),
	port_name varchar(255),
	country_name text,
	function text,
	coordinates text,
	formatted_address text
)