create or replace function cache_operation_units_for_unit_on_unit()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_owner_unit is not null
            and
            old.deleted = 0
        then
            update owner.unit set
                __operation_units_json__ = __operation_units_json__ - old.id::text,
                (
                    operation_units_ids
                ) = (
                    select
                            array_agg(source_row.id) as operation_units_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __operation_units_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.unit, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_owner_unit = owner.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                old.id_owner_unit = owner.unit.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_owner_unit is not distinct from old.id_owner_unit
        then
            return new;
        end if;

        if
            new.id_owner_unit is not distinct from old.id_owner_unit
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_owner_unit is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update owner.unit set
                __operation_units_json__ = cm_merge_json(
            __operation_units_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_owner_unit', new.id_owner_unit
            ),
            TG_OP
        ),
                (
                    operation_units_ids
                ) = (
                    select
                            array_agg(source_row.id) as operation_units_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __operation_units_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_owner_unit', new.id_owner_unit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.unit, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_owner_unit = owner.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_owner_unit = owner.unit.id;

            return new;
        end if;

        if
            old.id_owner_unit is not null
            and
            old.deleted = 0
        then
            update owner.unit set
                __operation_units_json__ = __operation_units_json__ - old.id::text,
                (
                    operation_units_ids
                ) = (
                    select
                            array_agg(source_row.id) as operation_units_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __operation_units_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.unit, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_owner_unit = owner.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                old.id_owner_unit = owner.unit.id;
        end if;

        if
            new.id_owner_unit is not null
            and
            new.deleted = 0
        then
            update owner.unit set
                __operation_units_json__ = cm_merge_json(
            __operation_units_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_owner_unit', new.id_owner_unit
            ),
            TG_OP
        ),
                (
                    operation_units_ids
                ) = (
                    select
                            array_agg(source_row.id) as operation_units_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __operation_units_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_owner_unit', new.id_owner_unit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.unit, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_owner_unit = owner.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_owner_unit = owner.unit.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_owner_unit is not null
            and
            new.deleted = 0
        then
            update owner.unit set
                __operation_units_json__ = cm_merge_json(
            __operation_units_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_owner_unit', new.id_owner_unit
            ),
            TG_OP
        ),
                (
                    operation_units_ids
                ) = (
                    select
                            array_agg(source_row.id) as operation_units_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __operation_units_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_owner_unit', new.id_owner_unit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.unit, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_owner_unit = owner.unit.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_owner_unit = owner.unit.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_operation_units_for_unit_on_unit
after insert or update of deleted, id_owner_unit or delete
on operation.unit
for each row
execute procedure cache_operation_units_for_unit_on_unit();