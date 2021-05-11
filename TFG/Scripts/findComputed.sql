SELECT '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' as table_fullname, a.name as column_name, definition
FROM SYS.computed_columns a
JOIN
sys.tables b
ON a.object_id = b.object_id
JOIN INFORMATION_SCHEMA.TABLES c
ON b.name = c.TABLE_NAME