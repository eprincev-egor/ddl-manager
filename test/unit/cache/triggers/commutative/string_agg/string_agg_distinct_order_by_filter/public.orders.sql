create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            coalesce(old.profit > 0, false)
        then
            update companies set
                orders_numbers_doc_number = cm_array_remove_one_element(
                    orders_numbers_doc_number,
                    old.doc_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct 
                            item.doc_number,
                            ', '
                            order by
                                item.doc_number asc nulls last
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        )
                    ) as item(doc_number)
                )
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.doc_number is not distinct from old.doc_number
            and
            new.id_client is not distinct from old.id_client
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                orders_numbers_doc_number = case
                    when
                        new.profit > 0
                        and
                        not coalesce(old.profit > 0, false)
                    then
                        array_append(
                            orders_numbers_doc_number,
                            new.doc_number
                        )
                    when
                        not coalesce(new.profit > 0, false)
                        and
                        old.profit > 0
                    then
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        )
                    else
                        array_append(
                            cm_array_remove_one_element(
                                orders_numbers_doc_number,
                                old.doc_number
                            ),
                            new.doc_number
                        )
                end,
                orders_numbers = case
                    when
                        new.profit > 0
                        and
                        not coalesce(old.profit > 0, false)
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.doc_number,
                                    ', '
                                    order by
                                        item.doc_number asc nulls last
                                )

                            from unnest(
                                array_append(
                                    orders_numbers_doc_number,
                                    new.doc_number
                                )
                            ) as item(doc_number)
                        )
                    when
                        not coalesce(new.profit > 0, false)
                        and
                        old.profit > 0
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.doc_number,
                                    ', '
                                    order by
                                        item.doc_number asc nulls last
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    orders_numbers_doc_number,
                                    old.doc_number
                                )
                            ) as item(doc_number)
                        )
                    else
                        (
                            select
                                string_agg(distinct 
                                    item.doc_number,
                                    ', '
                                    order by
                                        item.doc_number asc nulls last
                                )

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        orders_numbers_doc_number,
                                        old.doc_number
                                    ),
                                    new.doc_number
                                )
                            ) as item(doc_number)
                        )
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            coalesce(old.profit > 0, false)
        then
            update companies set
                orders_numbers_doc_number = cm_array_remove_one_element(
                    orders_numbers_doc_number,
                    old.doc_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct 
                            item.doc_number,
                            ', '
                            order by
                                item.doc_number asc nulls last
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        )
                    ) as item(doc_number)
                )
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            coalesce(new.profit > 0, false)
        then
            update companies set
                orders_numbers_doc_number = array_append(
                    orders_numbers_doc_number,
                    new.doc_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct 
                            item.doc_number,
                            ', '
                            order by
                                item.doc_number asc nulls last
                        )

                    from unnest(
                        array_append(
                            orders_numbers_doc_number,
                            new.doc_number
                        )
                    ) as item(doc_number)
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
            coalesce(new.profit > 0, false)
        then
            update companies set
                orders_numbers_doc_number = array_append(
                    orders_numbers_doc_number,
                    new.doc_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct 
                            item.doc_number,
                            ', '
                            order by
                                item.doc_number asc nulls last
                        )

                    from unnest(
                        array_append(
                            orders_numbers_doc_number,
                            new.doc_number
                        )
                    ) as item(doc_number)
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
after insert or update of doc_number, id_client, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();