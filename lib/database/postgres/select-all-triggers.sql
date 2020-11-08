select
    pg_get_triggerdef( pg_trigger.oid ) as ddl,
    pg_catalog.obj_description( pg_trigger.oid ) as comment
from pg_trigger
where
    pg_trigger.tgisinternal = false