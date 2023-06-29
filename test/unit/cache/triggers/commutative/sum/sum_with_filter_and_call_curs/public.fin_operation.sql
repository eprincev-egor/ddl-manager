create or replace function cache_totals_for_orders_on_fin_operation()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_order is not null
            and
            old.deleted = 0
            and
            (
                coalesce(old.id_fin_operation_type = 1, false)
                or
                coalesce(old.id_fin_operation_type = 2, false)
            )
        then
            update orders set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    fin_operation_buys,
                    fin_operation_sales
                ) = (
                    select
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 1) as fin_operation_buys,
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 2) as fin_operation_sales
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.fin_operation, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_order = orders.id
                        and
                        source_row.deleted = 0
                )
            where
                old.id_order = orders.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.date is not distinct from old.date
            and
            new.deleted is not distinct from old.deleted
            and
            new.id_currency is not distinct from old.id_currency
            and
            new.id_fin_operation_type is not distinct from old.id_fin_operation_type
            and
            new.id_order is not distinct from old.id_order
            and
            new.sum is not distinct from old.sum
        then
            return new;
        end if;

        if
            new.id_order is not distinct from old.id_order
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_order is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update orders set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'date', new.date,'deleted', new.deleted,'id', new.id,'id_currency', new.id_currency,'id_fin_operation_type', new.id_fin_operation_type,'id_order', new.id_order,'sum', new.sum
            ),
            TG_OP
        ),
                (
                    fin_operation_buys,
                    fin_operation_sales
                ) = (
                    select
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 1) as fin_operation_buys,
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 2) as fin_operation_sales
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'date', new.date,'deleted', new.deleted,'id', new.id,'id_currency', new.id_currency,'id_fin_operation_type', new.id_fin_operation_type,'id_order', new.id_order,'sum', new.sum
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.fin_operation, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_order = orders.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_order = orders.id;

            return new;
        end if;

        if
            old.id_order is not null
            and
            old.deleted = 0
            and
            (
                coalesce(old.id_fin_operation_type = 1, false)
                or
                coalesce(old.id_fin_operation_type = 2, false)
            )
        then
            update orders set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    fin_operation_buys,
                    fin_operation_sales
                ) = (
                    select
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 1) as fin_operation_buys,
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 2) as fin_operation_sales
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.fin_operation, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_order = orders.id
                        and
                        source_row.deleted = 0
                )
            where
                old.id_order = orders.id;
        end if;

        if
            new.id_order is not null
            and
            new.deleted = 0
            and
            (
                coalesce(new.id_fin_operation_type = 1, false)
                or
                coalesce(new.id_fin_operation_type = 2, false)
            )
        then
            update orders set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'date', new.date,'deleted', new.deleted,'id', new.id,'id_currency', new.id_currency,'id_fin_operation_type', new.id_fin_operation_type,'id_order', new.id_order,'sum', new.sum
            ),
            TG_OP
        ),
                (
                    fin_operation_buys,
                    fin_operation_sales
                ) = (
                    select
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 1) as fin_operation_buys,
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 2) as fin_operation_sales
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'date', new.date,'deleted', new.deleted,'id', new.id,'id_currency', new.id_currency,'id_fin_operation_type', new.id_fin_operation_type,'id_order', new.id_order,'sum', new.sum
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.fin_operation, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_order = orders.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_order is not null
            and
            new.deleted = 0
            and
            (
                coalesce(new.id_fin_operation_type = 1, false)
                or
                coalesce(new.id_fin_operation_type = 2, false)
            )
        then
            update orders set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'date', new.date,'deleted', new.deleted,'id', new.id,'id_currency', new.id_currency,'id_fin_operation_type', new.id_fin_operation_type,'id_order', new.id_order,'sum', new.sum
            ),
            TG_OP
        ),
                (
                    fin_operation_buys,
                    fin_operation_sales
                ) = (
                    select
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 1) as fin_operation_buys,
                            sum(
                                source_row.sum *     get_curs(
                                    source_row.date,
                                    source_row.id_currency
                                    )
                                                        ) filter (where     source_row.id_fin_operation_type = 2) as fin_operation_sales
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'date', new.date,'deleted', new.deleted,'id', new.id,'id_currency', new.id_currency,'id_fin_operation_type', new.id_fin_operation_type,'id_order', new.id_order,'sum', new.sum
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.fin_operation, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_order = orders.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_fin_operation
after insert or update of date, deleted, id_currency, id_fin_operation_type, id_order, sum or delete
on public.fin_operation
for each row
execute procedure cache_totals_for_orders_on_fin_operation();