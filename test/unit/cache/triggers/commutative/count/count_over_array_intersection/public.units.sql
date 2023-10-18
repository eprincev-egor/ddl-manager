create or replace function cache_totals_for_gtd_on_units()
returns trigger as $body$
declare inserted_orders_ids bigint[];
declare not_changed_orders_ids bigint[];
declare deleted_orders_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if old.orders_ids is not null then
            update gtd set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    units_count
                ) = (
                    select
                            count(*) as units_count
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids && cm_build_array_for((
                                        select orders_ids
                                        from public.units
                                        where false
                                    ), gtd.orders_ids)
                )
            where
                gtd.orders_ids && cm_build_array_for((
                                select orders_ids
                                from public.gtd
                                where false
                            ), old.orders_ids);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if cm_equal_arrays(new.orders_ids, old.orders_ids) then
            return new;
        end if;

        inserted_orders_ids = cm_get_inserted_elements(old.orders_ids, new.orders_ids);
        not_changed_orders_ids = cm_get_not_changed_elements(old.orders_ids, new.orders_ids);
        deleted_orders_ids = cm_get_deleted_elements(old.orders_ids, new.orders_ids);

        if not_changed_orders_ids is not null then
            update gtd set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'id', new.id,'orders_ids', new.orders_ids
            ),
            TG_OP
        ),
                (
                    units_count
                ) = (
                    select
                            count(*) as units_count
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'id', new.id,'orders_ids', new.orders_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids && cm_build_array_for((
                                        select orders_ids
                                        from public.units
                                        where false
                                    ), gtd.orders_ids)
                )
            where
                gtd.orders_ids && cm_build_array_for((
                                select orders_ids
                                from public.gtd
                                where false
                            ), not_changed_orders_ids);
        end if;

        if deleted_orders_ids is not null then
            update gtd set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    units_count
                ) = (
                    select
                            count(*) as units_count
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids && cm_build_array_for((
                                        select orders_ids
                                        from public.units
                                        where false
                                    ), gtd.orders_ids)
                )
            where
                gtd.orders_ids && cm_build_array_for((
                                select orders_ids
                                from public.gtd
                                where false
                            ), deleted_orders_ids);
        end if;

        if inserted_orders_ids is not null then
            update gtd set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'id', new.id,'orders_ids', new.orders_ids
            ),
            TG_OP
        ),
                (
                    units_count
                ) = (
                    select
                            count(*) as units_count
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'id', new.id,'orders_ids', new.orders_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids && cm_build_array_for((
                                        select orders_ids
                                        from public.units
                                        where false
                                    ), gtd.orders_ids)
                )
            where
                gtd.orders_ids && cm_build_array_for((
                                select orders_ids
                                from public.gtd
                                where false
                            ), inserted_orders_ids);
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.orders_ids is not null then
            update gtd set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'id', new.id,'orders_ids', new.orders_ids
            ),
            TG_OP
        ),
                (
                    units_count
                ) = (
                    select
                            count(*) as units_count
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'id', new.id,'orders_ids', new.orders_ids
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.units, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids && cm_build_array_for((
                                        select orders_ids
                                        from public.units
                                        where false
                                    ), gtd.orders_ids)
                )
            where
                gtd.orders_ids && cm_build_array_for((
                                select orders_ids
                                from public.gtd
                                where false
                            ), new.orders_ids);
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_gtd_on_units
after insert or update of orders_ids or delete
on public.units
for each row
execute procedure cache_totals_for_gtd_on_units();