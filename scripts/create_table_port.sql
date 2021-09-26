CREATE TABLE IF NOT EXISTS port (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	port_name text,
	country_name text,
	unlocode text,
	function text,
	coordinates text,
	is_failed_mapping int,
	formatted_address text
)