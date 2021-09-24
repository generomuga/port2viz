SELECT 
	c.id,
	c.unlocode,
	c.eta,
	c.etb,
	c.etd,
	c.quantity,
	c.material,
	p.function,
	p.port_name,
	p.country_name,
	p.function,
	p.coordinates,
	p.is_failed_mapping
FROM 
	cargoline c 
JOIN 
	port p 
ON 
	p.unlocode = c.unlocode
ORDER BY
	c.unlocode