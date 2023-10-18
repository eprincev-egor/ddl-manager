create or replace function cache_last_operation_for_units_on_operations()
returns trigger as $body$
declare inserted_units_ids bigint[];
declare not_changed_units_ids bigint[];
declare deleted_units_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if old.units_ids is not null then
            update units set
                __last_operation_json__ = __last_operation_json__ - old.id::text,
                (
                    last_operation_incoming_date,
                    last_operation_outgoing_date
                ) = (
                    select
                            source_row.incoming_date as last_operation_incoming_date,
                            source_row.outgoing_date as last_operation_outgoing_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    __last_operation_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((
                                        select units_ids
                                        from public.operations
                                        where false
                                    ), ARRAY[units.id]::bigint[])
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                units.id = any( old.units_ids::bigint[] );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.incoming_date is not distinct from old.incoming_date
            and
            new.outgoing_date is not distinct from old.outgoing_date
            and
            cm_equal_arrays(new.units_ids, old.units_ids)
        then
            return new;
        end if;

        inserted_units_ids = cm_get_inserted_elements(old.units_ids, new.units_ids);
        not_changed_units_ids = cm_get_not_changed_elements(old.units_ids, new.units_ids);
        deleted_units_ids = cm_get_deleted_elements(old.units_ids, new.units_ids);

        if not_changed_units_ids is not null then
            update units set
                __last_operation_json__ = cm_merge_json(
            __last_operation_json__,
            jsonb_build_object(
                'id', new.id,'incoming_date', new.incoming_date,'outgoing_date', new.outgoing_date,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    last_operation_incoming_date,
                    last_operation_outgoing_date
                ) = (
                    select
                            source_row.incoming_date as last_operation_incoming_date,
                            source_row.outgoing_date as last_operation_outgoing_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __last_operation_json__,
                jsonb_build_object(
                    'id', new.id,'incoming_date', new.incoming_date,'outgoing_date', new.outgoing_date,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((
                                        select units_ids
                                        from public.operations
                                        where false
                                    ), ARRAY[units.id]::bigint[])
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                units.id = any( not_changed_units_ids::bigint[] );
        end if;

        if deleted_units_ids is not null then
            update units set
                __last_operation_json__ = __last_operation_json__ - old.id::text,
                (
                    last_operation_incoming_date,
                    last_operation_outgoing_date
                ) = (
                    select
                            source_row.incoming_date as last_operation_incoming_date,
                            source_row.outgoing_date as last_operation_outgoing_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    __last_operation_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((
                                        select units_ids
                                        from public.operations
                                        where false
                                    ), ARRAY[units.id]::bigint[])
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                units.id = any( deleted_units_ids::bigint[] );
        end if;

        if inserted_units_ids is not null then
            update units set
                __last_operation_json__ = cm_merge_json(
            __last_operation_json__,
            jsonb_build_object(
                'id', new.id,'incoming_date', new.incoming_date,'outgoing_date', new.outgoing_date,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    last_operation_incoming_date,
                    last_operation_outgoing_date
                ) = (
                    select
                            source_row.incoming_date as last_operation_incoming_date,
                            source_row.outgoing_date as last_operation_outgoing_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __last_operation_json__,
                jsonb_build_object(
                    'id', new.id,'incoming_date', new.incoming_date,'outgoing_date', new.outgoing_date,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((
                                        select units_ids
                                        from public.operations
                                        where false
                                    ), ARRAY[units.id]::bigint[])
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                units.id = any( inserted_units_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.units_ids is not null then
            update units set
                __last_operation_json__ = cm_merge_json(
            __last_operation_json__,
            jsonb_build_object(
                'id', new.id,'incoming_date', new.incoming_date,'outgoing_date', new.outgoing_date,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    last_operation_incoming_date,
                    last_operation_outgoing_date
                ) = (
                    select
                            source_row.incoming_date as last_operation_incoming_date,
                            source_row.outgoing_date as last_operation_outgoing_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __last_operation_json__,
                jsonb_build_object(
                    'id', new.id,'incoming_date', new.incoming_date,'outgoing_date', new.outgoing_date,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((
                                        select units_ids
                                        from public.operations
                                        where false
                                    ), ARRAY[units.id]::bigint[])
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                units.id = any( new.units_ids::bigint[] );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_last_operation_for_units_on_operations
after insert or update of incoming_date, outgoing_date, units_ids or delete
on public.operations
for each row
execute procedure cache_last_operation_for_units_on_operations();