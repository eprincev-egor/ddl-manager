select
	distinct replace(aggfnoid::text, 'pg_catalog.', '') as agg_func_name
from pg_aggregate
order by replace(aggfnoid::text, 'pg_catalog.', '')