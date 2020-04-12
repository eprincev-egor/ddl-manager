select
    tc.constraint_type,
    tc.constraint_name,
    tc.table_schema,
    tc.table_name,
    tc.table_schema || '.' || tc.table_name as table_identify,
    rc.update_rule,
    rc.delete_rule,
    (
        select
            array_agg(kc.column_name::text)
        from information_schema.key_column_usage as kc
        where
            kc.table_name = tc.table_name and
            kc.table_schema = tc.table_schema and
            kc.constraint_name = tc.constraint_name
    ) as columns,
    fk_info.columns as reference_columns,
    fk_info.table_name as reference_table,
    check_info.check_clause

from information_schema.table_constraints as tc

left join information_schema.referential_constraints as rc on
    rc.constraint_schema = tc.constraint_schema and
    rc.constraint_name = tc.constraint_name

left join lateral (
    select
        ( 
            array_agg( distinct 
                ccu.table_schema::text || '.' ||
                ccu.table_name::text 
            ) 
        )[1] as table_name,
        array_agg( ccu.column_name::text ) as columns
    from information_schema.constraint_column_usage as ccu
    where
        ccu.constraint_name = rc.constraint_name and
        ccu.constraint_schema = rc.constraint_schema
) as fk_info on true

left join information_schema.check_constraints as check_info on
    check_info.constraint_schema = tc.table_schema and
    check_info.constraint_name = tc.constraint_name


where
    tc.table_schema != 'pg_catalog' and
    tc.table_schema != 'information_schema' and
    (
        tc.constraint_type != 'CHECK'
        or
        tc.constraint_name not ilike '%_not_null' and
        check_info.check_clause not ilike '% IS NOT NULL'
    )