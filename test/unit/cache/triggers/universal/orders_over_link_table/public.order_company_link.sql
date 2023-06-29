create or replace function cache_totals_for_companies_on_order_company_link()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_company is not null then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    max_order_date,
                    orders_numbers
                ) = (
                    select
                            max(orders.order_date) as max_order_date,
                            string_agg(distinct 
                                orders.order_number,
                                ', '
                                                        ) as orders_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.order_company_link, json_entry.value) as record on
                            true
                    ) as source_row

                    left join orders on
                        orders.id = source_row.id_order
                    where
                        source_row.id_company = companies.id
                )
            where
                old.id_company = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_company is not distinct from old.id_company
            and
            new.id_order is not distinct from old.id_order
        then
            return new;
        end if;

        if new.id_company is not distinct from old.id_company then
            if new.id_company is null then
                return new;
            end if;

            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_company', new.id_company,'id_order', new.id_order
            ),
            TG_OP
        ),
                (
                    max_order_date,
                    orders_numbers
                ) = (
                    select
                            max(orders.order_date) as max_order_date,
                            string_agg(distinct 
                                orders.order_number,
                                ', '
                                                        ) as orders_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_company', new.id_company,'id_order', new.id_order
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.order_company_link, json_entry.value) as record on
                            true
                    ) as source_row

                    left join orders on
                        orders.id = source_row.id_order
                    where
                        source_row.id_company = companies.id
                )
            where
                new.id_company = companies.id;

            return new;
        end if;

        if old.id_company is not null then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    max_order_date,
                    orders_numbers
                ) = (
                    select
                            max(orders.order_date) as max_order_date,
                            string_agg(distinct 
                                orders.order_number,
                                ', '
                                                        ) as orders_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.order_company_link, json_entry.value) as record on
                            true
                    ) as source_row

                    left join orders on
                        orders.id = source_row.id_order
                    where
                        source_row.id_company = companies.id
                )
            where
                old.id_company = companies.id;
        end if;

        if new.id_company is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_company', new.id_company,'id_order', new.id_order
            ),
            TG_OP
        ),
                (
                    max_order_date,
                    orders_numbers
                ) = (
                    select
                            max(orders.order_date) as max_order_date,
                            string_agg(distinct 
                                orders.order_number,
                                ', '
                                                        ) as orders_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_company', new.id_company,'id_order', new.id_order
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.order_company_link, json_entry.value) as record on
                            true
                    ) as source_row

                    left join orders on
                        orders.id = source_row.id_order
                    where
                        source_row.id_company = companies.id
                )
            where
                new.id_company = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_company is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_company', new.id_company,'id_order', new.id_order
            ),
            TG_OP
        ),
                (
                    max_order_date,
                    orders_numbers
                ) = (
                    select
                            max(orders.order_date) as max_order_date,
                            string_agg(distinct 
                                orders.order_number,
                                ', '
                                                        ) as orders_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_company', new.id_company,'id_order', new.id_order
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.order_company_link, json_entry.value) as record on
                            true
                    ) as source_row

                    left join orders on
                        orders.id = source_row.id_order
                    where
                        source_row.id_company = companies.id
                )
            where
                new.id_company = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_order_company_link
after insert or update of id_company, id_order or delete
on public.order_company_link
for each row
execute procedure cache_totals_for_companies_on_order_company_link();