SELECT object_name(i.object_id) AS ObjectName,
'DROP INDEX '+i.name+' ON '+DB_NAME()+'.'+SCHEMA_NAME(o.schema_id)+'.'+object_name(i.object_id)
FROM sys.indexes i 
INNER JOIN sys.objects o ON o.object_id = i.object_id
LEFT JOIN sys.dm_db_index_usage_stats s 
ON i.object_id=s.object_id AND i.index_id=s.index_id AND database_id = DB_ID() 
WHERE objectproperty(o.object_id,'IsUserTable') = 1 AND s.index_id IS NULL AND i.Name IS NOT NULL
AND i.Name NOT LIKE 'PK_%'