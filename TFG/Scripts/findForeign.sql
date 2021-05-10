select '['+schema_name(fk_tab.schema_id) + '].[' + fk_tab.name+']' as foreign_table,
        fk.name as fk_constraint_name,
        '['+schema_name(pk_tab.schema_id) + '].[' + pk_tab.name+']'
    from sys.foreign_keys fk
        inner join sys.tables fk_tab
            on fk_tab.object_id = fk.parent_object_id
        inner join sys.tables pk_tab
            on pk_tab.object_id = fk.referenced_object_id
        inner join sys.foreign_key_columns fk_cols
            on fk_cols.constraint_object_id = fk.object_id
			order by fk_constraint_name