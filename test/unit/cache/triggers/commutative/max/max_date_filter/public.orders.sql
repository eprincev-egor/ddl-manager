create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            (
                coalesce(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                or
                coalesce(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
            )
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    max_general_order_date,
                    max_combiner_order_date
                ) = (
                    select
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[1, 2, 3, 4] :: bigint[])) as max_general_order_date,
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[5, 6, 7, 8] :: bigint[])) as max_combiner_order_date
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
            new.id_order_type is not distinct from old.id_order_type
            and
            new.order_date is not distinct from old.order_date
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
                'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type,'order_date', new.order_date
            ),
            TG_OP
        ),
                (
                    max_general_order_date,
                    max_combiner_order_date
                ) = (
                    select
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[1, 2, 3, 4] :: bigint[])) as max_general_order_date,
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[5, 6, 7, 8] :: bigint[])) as max_combiner_order_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type,'order_date', new.order_date
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
                coalesce(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                or
                coalesce(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
            )
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    max_general_order_date,
                    max_combiner_order_date
                ) = (
                    select
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[1, 2, 3, 4] :: bigint[])) as max_general_order_date,
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[5, 6, 7, 8] :: bigint[])) as max_combiner_order_date
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
                coalesce(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                or
                coalesce(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
            )
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type,'order_date', new.order_date
            ),
            TG_OP
        ),
                (
                    max_general_order_date,
                    max_combiner_order_date
                ) = (
                    select
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[1, 2, 3, 4] :: bigint[])) as max_general_order_date,
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[5, 6, 7, 8] :: bigint[])) as max_combiner_order_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type,'order_date', new.order_date
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
                coalesce(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                or
                coalesce(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
            )
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type,'order_date', new.order_date
            ),
            TG_OP
        ),
                (
                    max_general_order_date,
                    max_combiner_order_date
                ) = (
                    select
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[1, 2, 3, 4] :: bigint[])) as max_general_order_date,
                            max(source_row.order_date) filter (where     source_row.id_order_type = any (array[5, 6, 7, 8] :: bigint[])) as max_combiner_order_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'id_order_type', new.id_order_type,'order_date', new.order_date
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
after insert or update of id_client, id_order_type, order_date or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();