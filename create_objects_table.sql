do $$
begin

    -- validate table if exists
    if exists(
        select from information_schema.tables
        where
            table_schema = 'public' and
            table_name = 'ddl_manager_objects'
    ) then
        -- check column: identify
        if not exists(
            select from information_schema.columns
            where
                table_schema = 'public' and
                table_name = 'ddl_manager_objects' and
                column_name = 'identify' and
                data_type = 'text'
        ) then
            raise exception 'invalid table ddl_manager_objects, no column identify or wrong type';
        end if;

        -- check column: type
        if not exists(
            select from information_schema.columns
            where
                table_schema = 'public' and
                table_name = 'ddl_manager_objects' and
                column_name = 'type' and
                data_type = 'text'
        ) then
            raise exception 'invalid table ddl_manager_objects, no column type';
        end if;

        -- check column: ddl
        if not exists(
            select from information_schema.columns
            where
                table_schema = 'public' and
                table_name = 'ddl_manager_objects' and
                column_name = 'ddl' and
                data_type = 'text'
        ) then
            raise exception 'invalid table ddl_manager_objects, no column type';
        end if;
    end if;


    if not exists(
        select from information_schema.tables
        where
            table_schema = 'public' and
            table_name = 'ddl_manager_objects'
    ) then
        create table ddl_manager_objects (
            identify text not null,
            type text not null check(
                type in ('trigger', 'function')
            ),
            ddl text not null,
            dt_create timestamp without time zone
                not null default now(),
            dt_update timestamp without time zone,
            constraint ddl_manager_objects_pk primary key (identify, type)
        );

        create index ddl_manager_objects_ddl_idx
        ON ddl_manager_objects
        using btree
        (ddl);

    end if;
end
$$