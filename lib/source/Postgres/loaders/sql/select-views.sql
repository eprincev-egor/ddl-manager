select
	'create or replace view ' || pg_views.viewname || '.' || pg_views.viewname || ' as ' || 
        pg_views.definition
from pg_catalog.pg_views
where
	schemaname != 'pg_catalog' and
	schemaname != 'information_schema'