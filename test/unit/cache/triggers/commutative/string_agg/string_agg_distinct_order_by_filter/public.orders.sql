create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            old.profit > 0
        then
            update companies set
                orders_numbers_doc_number = cm_array_remove_one_element(
                    orders_numbers_doc_number,
                    old.doc_number
                ),
                orders_numbers_profit = cm_array_remove_one_element(
                    orders_numbers_profit,
                    old.profit
                ),
                orders_numbers = (
                    select
                        string_agg(distinct 
                            item.doc_number,
                            ', '
                            order by
                                item.doc_number asc nulls last
                        ) filter (where orders.profit > 0)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        ),
                        cm_array_remove_one_element(
                            orders_numbers_profit,
                            old.profit
                        )
                    ) as item(doc_number, profit)
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
                        not(old.profit > 0)
                    then
                        array_append(
                            orders_numbers_doc_number,
                            new.doc_number
                        )
                    when
                        not(new.profit > 0)
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
                orders_numbers_profit = case
                    when
                        new.profit > 0
                        and
                        not(old.profit > 0)
                    then
                        array_append(
                            orders_numbers_profit,
                            new.profit
                        )
                    when
                        not(new.profit > 0)
                        and
                        old.profit > 0
                    then
                        cm_array_remove_one_element(
                            orders_numbers_profit,
                            old.profit
                        )
                    else
                        array_append(
                            cm_array_remove_one_element(
                                orders_numbers_profit,
                                old.profit
                            ),
                            new.profit
                        )
                end,
                orders_numbers = case
                    when
                        new.profit > 0
                        and
                        not(old.profit > 0)
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.doc_number,
                                    ', '
                                    order by
                                        item.doc_number asc nulls last
                                ) filter (where orders.profit > 0)

                            from unnest(
                                array_append(
                                    orders_numbers_doc_number,
                                    new.doc_number
                                ),
                                array_append(
                                    orders_numbers_profit,
                                    new.profit
                                )
                            ) as item(doc_number, profit)
                        )
                    when
                        not(new.profit > 0)
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
                                ) filter (where orders.profit > 0)

                            from unnest(
                                cm_array_remove_one_element(
                                    orders_numbers_doc_number,
                                    old.doc_number
                                ),
                                cm_array_remove_one_element(
                                    orders_numbers_profit,
                                    old.profit
                                )
                            ) as item(doc_number, profit)
                        )
                    else
                        (
                            select
                                string_agg(distinct 
                                    item.doc_number,
                                    ', '
                                    order by
                                        item.doc_number asc nulls last
                                ) filter (where orders.profit > 0)

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        orders_numbers_doc_number,
                                        old.doc_number
                                    ),
                                    new.doc_number
                                ),
                                array_append(
                                    cm_array_remove_one_element(
                                        orders_numbers_profit,
                                        old.profit
                                    ),
                                    new.profit
                                )
                            ) as item(doc_number, profit)
                        )
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            old.profit > 0
        then
            update companies set
                orders_numbers_doc_number = cm_array_remove_one_element(
                    orders_numbers_doc_number,
                    old.doc_number
                ),
                orders_numbers_profit = cm_array_remove_one_element(
                    orders_numbers_profit,
                    old.profit
                ),
                orders_numbers = (
                    select
                        string_agg(distinct 
                            item.doc_number,
                            ', '
                            order by
                                item.doc_number asc nulls last
                        ) filter (where orders.profit > 0)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        ),
                        cm_array_remove_one_element(
                            orders_numbers_profit,
                            old.profit
                        )
                    ) as item(doc_number, profit)
                )
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            new.profit > 0
        then
            update companies set
                orders_numbers_doc_number = array_append(
                    orders_numbers_doc_number,
                    new.doc_number
                ),
                orders_numbers_profit = array_append(
                    orders_numbers_profit,
                    new.profit
                ),
                orders_numbers = (
                    select
                        string_agg(distinct 
                            item.doc_number,
                            ', '
                            order by
                                item.doc_number asc nulls last
                        ) filter (where orders.profit > 0)

                    from unnest(
                        array_append(
                            orders_numbers_doc_number,
                            new.doc_number
                        ),
                        array_append(
                            orders_numbers_profit,
                            new.profit
                        )
                    ) as item(doc_number, profit)
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
            new.profit > 0
        then
            update companies set
                orders_numbers_doc_number = array_append(
                    orders_numbers_doc_number,
                    new.doc_number
                ),
                orders_numbers_profit = array_append(
                    orders_numbers_profit,
                    new.profit
                ),
                orders_numbers = (
                    select
                        string_agg(distinct 
                            item.doc_number,
                            ', '
                            order by
                                item.doc_number asc nulls last
                        ) filter (where orders.profit > 0)

                    from unnest(
                        array_append(
                            orders_numbers_doc_number,
                            new.doc_number
                        ),
                        array_append(
                            orders_numbers_profit,
                            new.profit
                        )
                    ) as item(doc_number, profit)
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