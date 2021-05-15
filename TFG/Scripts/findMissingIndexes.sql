SELECT 	REPLACE(REPLACE('CREATE NONCLUSTERED INDEX IX_'+REPLACE(REPLACE(REPLACE([statement],'[',''),'.',''),']','')+'_'+CONVERT(VARCHAR,b.INDEX_HANDLE)+'_'+CONVERT(VARCHAR,C.DATABASE_ID)+'_'+CONVERT(VARCHAR,OBJECT_ID)+'_'++' ON '+isnull([statement],'')+' ('+isnull([equality_columns],'')+','+isnull([inequality_columns],'')+') INCLUDE ('+isnull([included_columns],'')+') ON [PRIMARY];',',)',')'),'(,','(')
FROM sys.dm_db_missing_index_details C
	INNER JOIN sys.dm_db_missing_index_groups B
		ON B.INDEX_HANDLE=C.INDEX_HANDLE
	INNER JOIN sys.dm_db_missing_index_group_stats A
		ON A.GROUP_HANDLE=B.INDEX_GROUP_HANDLE
WHERE a.avg_total_user_cost * a.avg_user_impact * (a.user_seeks + a.user_scans)>100