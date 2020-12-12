create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            old.doc_number is not null
            and
            old.profit > 0
        then
            update companies set
                orders_numbers_array_agg = cm_array_remove_one_element(
                    orders_numbers_array_agg,
                    old.doc_number
                ),
                orders_numbers = cm_array_to_string_distinct(
                    cm_array_remove_one_element(
                        orders_numbers_array_agg,
                        old.doc_number
                    ),
                    ', '
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
                orders_numbers_array_agg = case
                    when
                        new.profit > 0
                        and
                        not(old.profit > 0)
                    then
                        cm_array_append_order_by_asc_nulls_last(
                            orders_numbers_array_agg,
                            new.doc_number
                        )
                    when
                        not(new.profit > 0)
                        and
                        old.profit > 0
                    then
                        cm_array_remove_one_element(
                            orders_numbers_array_agg,
                            old.doc_number
                        )
                    else
                        cm_array_append_order_by_asc_nulls_last(
                            cm_array_remove_one_element(
                                orders_numbers_array_agg,
                                old.doc_number
                            ),
                            new.doc_number
                        )
                end,
                orders_numbers = case
                    when
                        new.profit > 0
                        and
                        not(old.profit > 0)
                    then
                        cm_array_to_string_distinct(
                            cm_array_append_order_by_asc_nulls_last(
                                orders_numbers_array_agg,
                                new.doc_number
                            ),
                            ', '
                        )
                    when
                        not(new.profit > 0)
                        and
                        old.profit > 0
                    then
                        cm_array_to_string_distinct(
                            cm_array_remove_one_element(
                                orders_numbers_array_agg,
                                old.doc_number
                            ),
                            ', '
                        )
                    else
                        cm_array_to_string_distinct(
                            cm_array_append_order_by_asc_nulls_last(
                                cm_array_remove_one_element(
                                    orders_numbers_array_agg,
                                    old.doc_number
                                ),
                                new.doc_number
                            ),
                            ', '
                        )
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            old.doc_number is not null
            and
            old.profit > 0
        then
            update companies set
                orders_numbers_array_agg = cm_array_remove_one_element(
                    orders_numbers_array_agg,
                    old.doc_number
                ),
                orders_numbers = cm_array_to_string_distinct(
                    cm_array_remove_one_element(
                        orders_numbers_array_agg,
                        old.doc_number
                    ),
                    ', '
                )
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            new.doc_number is not null
            and
            new.profit > 0
        then
            update companies set
                orders_numbers_array_agg = cm_array_append_order_by_asc_nulls_last(
                    orders_numbers_array_agg,
                    new.doc_number
                ),
                orders_numbers = cm_array_to_string_distinct(
                    cm_array_append_order_by_asc_nulls_last(
                        orders_numbers_array_agg,
                        new.doc_number
                    ),
                    ', '
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
            new.doc_number is not null
            and
            new.profit > 0
        then
            update companies set
                orders_numbers_array_agg = cm_array_append_order_by_asc_nulls_last(
                    orders_numbers_array_agg,
                    new.doc_number
                ),
                orders_numbers = cm_array_to_string_distinct(
                    cm_array_append_order_by_asc_nulls_last(
                        orders_numbers_array_agg,
                        new.doc_number
                    ),
                    ', '
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