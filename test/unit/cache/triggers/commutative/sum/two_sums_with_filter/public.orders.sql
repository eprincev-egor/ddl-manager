create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            (
                coalesce(old.is_sale, false)
                or
                coalesce(old.is_buy, false)
            )
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    orders_profit
                ) = (
                    select
                            sum(source_row.total) filter (where     source_row.is_sale) -                             sum(source_row.total) filter (where     source_row.is_buy) as orders_profit
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
            new.id_client is not distinct from old.id_client
            and
            new.is_buy is not distinct from old.is_buy
            and
            new.is_sale is not distinct from old.is_sale
            and
            new.total is not distinct from old.total
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
                'id', new.id,'id_client', new.id_client,'is_buy', new.is_buy,'is_sale', new.is_sale,'total', new.total
            ),
            TG_OP
        ),
                (
                    orders_profit
                ) = (
                    select
                            sum(source_row.total) filter (where     source_row.is_sale) -                             sum(source_row.total) filter (where     source_row.is_buy) as orders_profit
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'is_buy', new.is_buy,'is_sale', new.is_sale,'total', new.total
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
            (
                coalesce(old.is_sale, false)
                or
                coalesce(old.is_buy, false)
            )
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    orders_profit
                ) = (
                    select
                            sum(source_row.total) filter (where     source_row.is_sale) -                             sum(source_row.total) filter (where     source_row.is_buy) as orders_profit
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
            (
                coalesce(new.is_sale, false)
                or
                coalesce(new.is_buy, false)
            )
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_client', new.id_client,'is_buy', new.is_buy,'is_sale', new.is_sale,'total', new.total
            ),
            TG_OP
        ),
                (
                    orders_profit
                ) = (
                    select
                            sum(source_row.total) filter (where     source_row.is_sale) -                             sum(source_row.total) filter (where     source_row.is_buy) as orders_profit
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'is_buy', new.is_buy,'is_sale', new.is_sale,'total', new.total
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
            (
                coalesce(new.is_sale, false)
                or
                coalesce(new.is_buy, false)
            )
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_client', new.id_client,'is_buy', new.is_buy,'is_sale', new.is_sale,'total', new.total
            ),
            TG_OP
        ),
                (
                    orders_profit
                ) = (
                    select
                            sum(source_row.total) filter (where     source_row.is_sale) -                             sum(source_row.total) filter (where     source_row.is_buy) as orders_profit
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'is_buy', new.is_buy,'is_sale', new.is_sale,'total', new.total
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
after insert or update of id_client, is_buy, is_sale, total or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();