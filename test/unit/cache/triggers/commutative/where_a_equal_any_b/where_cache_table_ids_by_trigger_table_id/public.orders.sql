create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.doc_number is not null
            and
            old.deleted = 0
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
                companies.order_ids && ARRAY[ old.id ];
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.doc_number is not distinct from old.doc_number
        then
            return new;
        end if;

        if new.deleted is not distinct from old.deleted then
            update companies set
                orders_numbers_array_agg = array_append(
                    cm_array_remove_one_element(
                        orders_numbers_array_agg,
                        old.doc_number
                    ),
                    new.doc_number
                ),
                orders_numbers = cm_array_to_string_distinct(
                    array_append(
                        cm_array_remove_one_element(
                            orders_numbers_array_agg,
                            old.doc_number
                        ),
                        new.doc_number
                    ),
                    ', '
                )
            where
                companies.order_ids && ARRAY[ new.id ];

            return new;
        end if;

        if
            old.doc_number is not null
            and
            old.deleted = 0
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
                companies.order_ids && ARRAY[ old.id ];
        end if;

        if
            new.doc_number is not null
            and
            new.deleted = 0
        then
            update companies set
                orders_numbers_array_agg = array_append(
                    orders_numbers_array_agg,
                    new.doc_number
                ),
                orders_numbers = cm_array_to_string_distinct(
                    array_append(
                        orders_numbers_array_agg,
                        new.doc_number
                    ),
                    ', '
                )
            where
                companies.order_ids && ARRAY[ new.id ];
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.doc_number is not null
            and
            new.deleted = 0
        then
            update companies set
                orders_numbers_array_agg = array_append(
                    orders_numbers_array_agg,
                    new.doc_number
                ),
                orders_numbers = cm_array_to_string_distinct(
                    array_append(
                        orders_numbers_array_agg,
                        new.doc_number
                    ),
                    ', '
                )
            where
                companies.order_ids && ARRAY[ new.id ];
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of deleted, doc_number or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();