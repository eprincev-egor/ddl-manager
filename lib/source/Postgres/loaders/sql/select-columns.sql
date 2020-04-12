select
    pg_columns.table_schema,
    pg_columns.table_name,
    pg_columns.table_schema || '.' || pg_columns.table_name as table_identify,
    pg_columns.column_name,
    pg_columns.column_default,
    pg_columns.data_type,
    pg_columns.is_nullable
from information_schema.columns as pg_columns
where
    pg_columns.table_schema != 'pg_catalog' and
    pg_columns.table_schema != 'information_schema'

order by pg_columns.ordinal_position