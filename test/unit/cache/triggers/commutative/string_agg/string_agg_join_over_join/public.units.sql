create or replace function cache_totals_for_orders_on_units()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_order is not null then
            update orders set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    units_types,
                    units_categories
                ) = (
                    select
                            string_agg(distinct unit_type.name, ', ') as units_types,
                            string_agg(distinct unit_category.name, ', ') as units_categories
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row

                    left join unit_type on
                        unit_type.id = source_row.id_unit_type

                    left join unit_category on
                        unit_category.id = unit_type.id_category
                    where
                        source_row.id_order = orders.id
                )
            where
                old.id_order = orders.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_order is not distinct from old.id_order
            and
            new.id_unit_type is not distinct from old.id_unit_type
        then
            return new;
        end if;

        if new.id_order is not distinct from old.id_order then
            if new.id_order is null then
                return new;
            end if;

            update orders set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_order', new.id_order,'id_unit_type', new.id_unit_type
            ),
            TG_OP
        ),
                (
                    units_types,
                    units_categories
                ) = (
                    select
                            string_agg(distinct unit_type.name, ', ') as units_types,
                            string_agg(distinct unit_category.name, ', ') as units_categories
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_order', new.id_order,'id_unit_type', new.id_unit_type
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row

                    left join unit_type on
                        unit_type.id = source_row.id_unit_type

                    left join unit_category on
                        unit_category.id = unit_type.id_category
                    where
                        source_row.id_order = orders.id
                )
            where
                new.id_order = orders.id;

            return new;
        end if;

        if old.id_order is not null then
            update orders set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    units_types,
                    units_categories
                ) = (
                    select
                            string_agg(distinct unit_type.name, ', ') as units_types,
                            string_agg(distinct unit_category.name, ', ') as units_categories
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row

                    left join unit_type on
                        unit_type.id = source_row.id_unit_type

                    left join unit_category on
                        unit_category.id = unit_type.id_category
                    where
                        source_row.id_order = orders.id
                )
            where
                old.id_order = orders.id;
        end if;

        if new.id_order is not null then
            update orders set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_order', new.id_order,'id_unit_type', new.id_unit_type
            ),
            TG_OP
        ),
                (
                    units_types,
                    units_categories
                ) = (
                    select
                            string_agg(distinct unit_type.name, ', ') as units_types,
                            string_agg(distinct unit_category.name, ', ') as units_categories
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_order', new.id_order,'id_unit_type', new.id_unit_type
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row

                    left join unit_type on
                        unit_type.id = source_row.id_unit_type

                    left join unit_category on
                        unit_category.id = unit_type.id_category
                    where
                        source_row.id_order = orders.id
                )
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_order is not null then
            update orders set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_order', new.id_order,'id_unit_type', new.id_unit_type
            ),
            TG_OP
        ),
                (
                    units_types,
                    units_categories
                ) = (
                    select
                            string_agg(distinct unit_type.name, ', ') as units_types,
                            string_agg(distinct unit_category.name, ', ') as units_categories
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_order', new.id_order,'id_unit_type', new.id_unit_type
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row

                    left join unit_type on
                        unit_type.id = source_row.id_unit_type

                    left join unit_category on
                        unit_category.id = unit_type.id_category
                    where
                        source_row.id_order = orders.id
                )
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_units
after insert or update of id_order, id_unit_type or delete
on public.units
for each row
execute procedure cache_totals_for_orders_on_units();