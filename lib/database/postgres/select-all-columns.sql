
select
    pg_columns.table_schema,
    pg_columns.table_name,
    pg_columns.table_schema || '.' || pg_columns.table_name as table_identify,
    pg_columns.column_name,
    pg_columns.column_default,
    pg_type.oid as column_type_oid, -- TODO: to_regtype(pg_columns.udt_name)
    pg_columns.is_nullable,
    (
        SELECT
            pg_description.description
        FROM pg_attribute

        left join pg_catalog.pg_description on
            pg_description.objoid = pg_attribute.attrelid and
            pg_description.objsubid = pg_attribute.attnum

        WHERE 
            pg_attribute.attrelid = (pg_columns.table_schema || '.' || pg_columns.table_name)::regclass and 
            pg_attribute.attname = pg_columns.column_name
    ) as comment
from information_schema.columns as pg_columns

left join pg_type on
    pg_type.typname = pg_columns.udt_name

where
    pg_columns.table_schema != 'pg_catalog' and
    pg_columns.table_schema != 'information_schema'

order by pg_columns.ordinal_position