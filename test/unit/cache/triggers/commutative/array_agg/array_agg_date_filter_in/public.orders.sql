create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            coalesce(old.id_order_type in (1, 2, 3), false)
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    general_orders_dates
                ) = (
                    select
                            array_agg(source_row.date) filter (where     source_row.id_order_type in (1, 2, 3)) as general_orders_dates
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_client = companies.id
                )
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.date is not distinct from old.date
            and
            new.id_client is not distinct from old.id_client
            and
            new.id_order_type is not distinct from old.id_order_type
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            if new.id_client is null then
                return new;
            end if;

            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
            'date', new.date,'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type
        ),
            TG_OP
        ),
                (
                    general_orders_dates
                ) = (
                    select
                            array_agg(source_row.date) filter (where     source_row.id_order_type in (1, 2, 3)) as general_orders_dates
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                'date', new.date,'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_client = companies.id
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            coalesce(old.id_order_type in (1, 2, 3), false)
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    general_orders_dates
                ) = (
                    select
                            array_agg(source_row.date) filter (where     source_row.id_order_type in (1, 2, 3)) as general_orders_dates
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_client = companies.id
                )
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            coalesce(new.id_order_type in (1, 2, 3), false)
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
            'date', new.date,'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type
        ),
            TG_OP
        ),
                (
                    general_orders_dates
                ) = (
                    select
                            array_agg(source_row.date) filter (where     source_row.id_order_type in (1, 2, 3)) as general_orders_dates
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                'date', new.date,'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_client = companies.id
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_client is not null
            and
            coalesce(new.id_order_type in (1, 2, 3), false)
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
            'date', new.date,'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type
        ),
            TG_OP
        ),
                (
                    general_orders_dates
                ) = (
                    select
                            array_agg(source_row.date) filter (where     source_row.id_order_type in (1, 2, 3)) as general_orders_dates
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                'date', new.date,'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_client = companies.id
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of date, id_client, id_order_type or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();