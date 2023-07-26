create or replace function cache_last_auto_doc_for_units_on_operations()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_units_ids bigint[];
declare not_changed_units_ids bigint[];
declare deleted_units_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if
            old.id_doc_parent_operation is not null
            and
            old.units_ids is not null
            and
            old.id_operation_type = 1
            and
            old.deleted = 0
        then
            update units set
                __last_auto_doc_json__ = __last_auto_doc_json__ - old.id::text,
                (
                    id_last_auto_doc
                ) = (
                    select
                            source_row.id as id_last_auto_doc
                    from (
                        select
                                record.*
                        from jsonb_each(
    __last_auto_doc_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_doc_parent_operation = units.id_last_auto
                        and
                        source_row.id_operation_type = 1
                        and
                        source_row.deleted = 0
                        and
                        source_row.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                old.id_doc_parent_operation = units.id_last_auto
                and
                units.id = any( old.units_ids::bigint[] );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_doc_parent_operation is not distinct from old.id_doc_parent_operation
            and
            new.id_operation_type is not distinct from old.id_operation_type
            and
            cm_equal_arrays(new.units_ids, old.units_ids)
        then
            return new;
        end if;

        matched_old = coalesce(old.id_operation_type = 1
            and
            old.deleted = 0, false);
        matched_new = coalesce(new.id_operation_type = 1
            and
            new.deleted = 0, false);

        if
            not matched_old
            and
            not matched_new
        then
            return new;
        end if;

        if
            matched_old
            and
            not matched_new
        then
            inserted_units_ids = null;
            not_changed_units_ids = null;
            deleted_units_ids = old.units_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_units_ids = new.units_ids;
            not_changed_units_ids = null;
            deleted_units_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_units_ids = cm_get_inserted_elements(old.units_ids, new.units_ids);
            not_changed_units_ids = cm_get_not_changed_elements(old.units_ids, new.units_ids);
            deleted_units_ids = cm_get_deleted_elements(old.units_ids, new.units_ids);
        end if;

        if
            new.id_doc_parent_operation is not null
            and
            not_changed_units_ids is not null
        then
            update units set
                __last_auto_doc_json__ = cm_merge_json(
            __last_auto_doc_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_doc_parent_operation', new.id_doc_parent_operation,'id_operation_type', new.id_operation_type,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    id_last_auto_doc
                ) = (
                    select
                            source_row.id as id_last_auto_doc
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __last_auto_doc_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_doc_parent_operation', new.id_doc_parent_operation,'id_operation_type', new.id_operation_type,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_doc_parent_operation = units.id_last_auto
                        and
                        source_row.id_operation_type = 1
                        and
                        source_row.deleted = 0
                        and
                        source_row.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                new.id_doc_parent_operation = units.id_last_auto
                and
                units.id = any( not_changed_units_ids::bigint[] );
        end if;

        if
            new.id_doc_parent_operation is not null
            and
            deleted_units_ids is not null
        then
            update units set
                __last_auto_doc_json__ = __last_auto_doc_json__ - old.id::text,
                (
                    id_last_auto_doc
                ) = (
                    select
                            source_row.id as id_last_auto_doc
                    from (
                        select
                                record.*
                        from jsonb_each(
    __last_auto_doc_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_doc_parent_operation = units.id_last_auto
                        and
                        source_row.id_operation_type = 1
                        and
                        source_row.deleted = 0
                        and
                        source_row.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                old.id_doc_parent_operation = units.id_last_auto
                and
                units.id = any( deleted_units_ids::bigint[] );
        end if;

        if
            new.id_doc_parent_operation is not null
            and
            inserted_units_ids is not null
        then
            update units set
                __last_auto_doc_json__ = cm_merge_json(
            __last_auto_doc_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_doc_parent_operation', new.id_doc_parent_operation,'id_operation_type', new.id_operation_type,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    id_last_auto_doc
                ) = (
                    select
                            source_row.id as id_last_auto_doc
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __last_auto_doc_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_doc_parent_operation', new.id_doc_parent_operation,'id_operation_type', new.id_operation_type,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_doc_parent_operation = units.id_last_auto
                        and
                        source_row.id_operation_type = 1
                        and
                        source_row.deleted = 0
                        and
                        source_row.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                new.id_doc_parent_operation = units.id_last_auto
                and
                units.id = any( inserted_units_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_doc_parent_operation is not null
            and
            new.units_ids is not null
            and
            new.id_operation_type = 1
            and
            new.deleted = 0
        then
            update units set
                __last_auto_doc_json__ = cm_merge_json(
            __last_auto_doc_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_doc_parent_operation', new.id_doc_parent_operation,'id_operation_type', new.id_operation_type,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    id_last_auto_doc
                ) = (
                    select
                            source_row.id as id_last_auto_doc
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __last_auto_doc_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_doc_parent_operation', new.id_doc_parent_operation,'id_operation_type', new.id_operation_type,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.operations, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_doc_parent_operation = units.id_last_auto
                        and
                        source_row.id_operation_type = 1
                        and
                        source_row.deleted = 0
                        and
                        source_row.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        source_row.id desc nulls first
                    limit 1
                )
            where
                new.id_doc_parent_operation = units.id_last_auto
                and
                units.id = any( new.units_ids::bigint[] );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_last_auto_doc_for_units_on_operations
after insert or update of deleted, id_doc_parent_operation, id_operation_type, units_ids or delete
on public.operations
for each row
execute procedure cache_last_auto_doc_for_units_on_operations();