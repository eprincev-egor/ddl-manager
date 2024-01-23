select
    pg_get_functiondef( obj.oid ) as ddl,
    pg_catalog.obj_description( obj.oid ) as comment
from information_schema.routines as routines

left join lateral (
    select
        (regexp_match(routines.specific_name, '_([0-9]+)$'))[1]::oid as oid
) as obj on true

left join pg_catalog.pg_proc as pg_proc on
    routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text

left join pg_catalog.pg_language as pg_language on
    pg_language.oid = pg_proc.prolang 

where
    routines.routine_schema <> 'pg_catalog' and
    routines.routine_schema <> 'information_schema' and
    routines.routine_definition is distinct from 'aggregate_dummy' and
    -- ignore language C or JS
    lower( coalesce(pg_language.lanname, 'plpgsql') ) in ('sql', 'plpgsql')
    and
    (
        pg_catalog.obj_description( obj.oid ) like '%ddl-manager-sync%' or
        not exists(
            select from pg_catalog.pg_aggregate as pg_aggregate
            where
                pg_aggregate.aggtransfn = obj.oid or
                pg_aggregate.aggfinalfn = obj.oid
        )
    )

order by
    routines.routine_schema, 
    routines.routine_name