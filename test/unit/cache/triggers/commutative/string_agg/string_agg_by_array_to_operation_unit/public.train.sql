create or replace function cache_trains_for_unit_on_train()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_units_ids bigint[];
declare not_changed_units_ids bigint[];
declare deleted_units_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if
            old.units_ids is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                __trains_json__ = __trains_json__ - old.id::text,
                (
                    train_numbers
                ) = (
                    select
                            string_agg(distinct source_row.number, ', ') as train_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    __trains_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((null::public.train).units_ids, ARRAY[operation.unit.id]::bigint[])
                        and
                        source_row.deleted = 0
                )
            where
                operation.unit.id = any( old.units_ids::bigint[] );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.number is not distinct from old.number
            and
            cm_equal_arrays(new.units_ids, old.units_ids)
        then
            return new;
        end if;

        matched_old = coalesce(old.deleted = 0, false);
        matched_new = coalesce(new.deleted = 0, false);

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

        if not_changed_units_ids is not null then
            update operation.unit set
                __trains_json__ = cm_merge_json(
            __trains_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'number', new.number,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    train_numbers
                ) = (
                    select
                            string_agg(distinct source_row.number, ', ') as train_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __trains_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'number', new.number,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((null::public.train).units_ids, ARRAY[operation.unit.id]::bigint[])
                        and
                        source_row.deleted = 0
                )
            where
                operation.unit.id = any( not_changed_units_ids::bigint[] );
        end if;

        if deleted_units_ids is not null then
            update operation.unit set
                __trains_json__ = __trains_json__ - old.id::text,
                (
                    train_numbers
                ) = (
                    select
                            string_agg(distinct source_row.number, ', ') as train_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    __trains_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((null::public.train).units_ids, ARRAY[operation.unit.id]::bigint[])
                        and
                        source_row.deleted = 0
                )
            where
                operation.unit.id = any( deleted_units_ids::bigint[] );
        end if;

        if inserted_units_ids is not null then
            update operation.unit set
                __trains_json__ = cm_merge_json(
            __trains_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'number', new.number,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    train_numbers
                ) = (
                    select
                            string_agg(distinct source_row.number, ', ') as train_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __trains_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'number', new.number,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((null::public.train).units_ids, ARRAY[operation.unit.id]::bigint[])
                        and
                        source_row.deleted = 0
                )
            where
                operation.unit.id = any( inserted_units_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.units_ids is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                __trains_json__ = cm_merge_json(
            __trains_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'number', new.number,'units_ids', new.units_ids
            ),
            TG_OP
        ),
                (
                    train_numbers
                ) = (
                    select
                            string_agg(distinct source_row.number, ', ') as train_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __trains_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'number', new.number,'units_ids', new.units_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.units_ids && cm_build_array_for((null::public.train).units_ids, ARRAY[operation.unit.id]::bigint[])
                        and
                        source_row.deleted = 0
                )
            where
                operation.unit.id = any( new.units_ids::bigint[] );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_trains_for_unit_on_train
after insert or update of deleted, number, units_ids or delete
on public.train
for each row
execute procedure cache_trains_for_unit_on_train();