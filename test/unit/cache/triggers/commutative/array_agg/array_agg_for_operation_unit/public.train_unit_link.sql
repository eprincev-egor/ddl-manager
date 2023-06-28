create or replace function cache_trains_ids_for_unit_on_train_unit_link()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_unit is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                __trains_ids_json__ = __trains_ids_json__ - old.id::text,
                (
                    trains_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_train) as trains_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __trains_ids_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_unit = operation.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                old.id_unit = operation.unit.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_train is not distinct from old.id_train
            and
            new.id_unit is not distinct from old.id_unit
        then
            return new;
        end if;

        if
            new.id_unit is not distinct from old.id_unit
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_unit is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update operation.unit set
                __trains_ids_json__ = cm_merge_json(
            __trains_ids_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_train', new.id_train,'id_unit', new.id_unit
        ),
            TG_OP
        ),
                (
                    trains_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_train) as trains_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __trains_ids_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_train', new.id_train,'id_unit', new.id_unit
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_unit = operation.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_unit = operation.unit.id;

            return new;
        end if;

        if
            old.id_unit is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                __trains_ids_json__ = __trains_ids_json__ - old.id::text,
                (
                    trains_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_train) as trains_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __trains_ids_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_unit = operation.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                old.id_unit = operation.unit.id;
        end if;

        if
            new.id_unit is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                __trains_ids_json__ = cm_merge_json(
            __trains_ids_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_train', new.id_train,'id_unit', new.id_unit
        ),
            TG_OP
        ),
                (
                    trains_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_train) as trains_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __trains_ids_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_train', new.id_train,'id_unit', new.id_unit
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_unit = operation.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_unit = operation.unit.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_unit is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                __trains_ids_json__ = cm_merge_json(
            __trains_ids_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_train', new.id_train,'id_unit', new.id_unit
        ),
            TG_OP
        ),
                (
                    trains_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_train) as trains_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __trains_ids_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_train', new.id_train,'id_unit', new.id_unit
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.train_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_unit = operation.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_unit = operation.unit.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_trains_ids_for_unit_on_train_unit_link
after insert or update of deleted, id_train, id_unit or delete
on public.train_unit_link
for each row
execute procedure cache_trains_ids_for_unit_on_train_unit_link();