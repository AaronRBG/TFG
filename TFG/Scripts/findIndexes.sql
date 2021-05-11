SELECT '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' as table_fullname, a.name
FROM SYS.indexes a
JOIN
sys.tables b
ON a.object_id = b.object_id
JOIN INFORMATION_SCHEMA.TABLES c
ON b.name = c.TABLE_NAME
WHERE a.name LIKE 'IX%' OR a.name LIKE 'AK%'