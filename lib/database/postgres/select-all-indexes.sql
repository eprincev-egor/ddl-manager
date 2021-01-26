 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    i.relname AS indexname,
    t.spcname AS tablespace,
    pg_get_indexdef(i.oid) AS indexdef,
    obj_description(i.oid) as comment
   FROM pg_index x
     JOIN pg_class c ON c.oid = x.indrelid
     JOIN pg_class i ON i.oid = x.indexrelid
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace
  WHERE
	(c.relkind = ANY (ARRAY['r'::"char", 'm'::"char"])) AND
	i.relkind = 'i'::"char" and
	n.nspname <> 'pg_catalog' and
	n.nspname <> 'information_schema'
  