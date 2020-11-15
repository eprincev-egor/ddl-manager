select
    pg_get_functiondef( pg_proc.oid ) as ddl,
    pg_catalog.obj_description( pg_proc.oid ) as comment
from information_schema.routines as routines

left join pg_catalog.pg_proc as pg_proc on
    routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text

-- ignore language C or JS
inner join pg_catalog.pg_language as pg_language on
    pg_language.oid = pg_proc.prolang and
    lower(pg_language.lanname) in ('sql', 'plpgsql')

where
    routines.routine_schema <> 'pg_catalog' and
    routines.routine_schema <> 'information_schema' and
    routines.routine_definition is distinct from 'aggregate_dummy' and
    pg_catalog.obj_description( pg_proc.oid ) is distinct from 'ddl-manager-cache' and
    not exists(
        select from pg_catalog.pg_aggregate as pg_aggregate
        where
            pg_aggregate.aggtransfn = pg_proc.oid or
            pg_aggregate.aggfinalfn = pg_proc.oid
    )

order by
    routines.routine_schema, 
    routines.routine_name