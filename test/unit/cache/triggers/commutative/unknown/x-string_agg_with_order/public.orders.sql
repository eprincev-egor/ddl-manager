create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
        then
            update companies set
                orders_numbers_array_agg = cm_array_remove_one_element(
                    orders_numbers_array_agg,
                    json_build_object(
                        'doc_number', old.doc_number,
                        'doc_date', old.doc_date
                    )
                ),
                orders_numbers = (
                    select
                        string_agg(
                            distinct input_row.doc_number,
                            ', '
                            
                            order by input_row.doc_date
                        )
                    from unnest( cm_array_remove_one_element(
                        orders_numbers_array_agg,
                        old.doc_number
                    ) ) as input_row
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
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                orders_numbers_array_agg = array_append(
                    cm_array_remove_one_element(
                        orders_numbers_array_agg,
                        old.doc_number
                    ),
                    new.doc_number
                ),
                orders_numbers = array_to_string(
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
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            old.doc_number is not null
        then
            update companies set
                orders_numbers_array_agg = cm_array_remove_one_element(
                    orders_numbers_array_agg,
                    old.doc_number
                ),
                orders_numbers = array_to_string(
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
        then
            update companies set
                orders_numbers_array_agg = array_append(
                    orders_numbers_array_agg,
                    new.doc_number
                ),
                orders_numbers = array_to_string(
                    array_append(
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
        then
            update companies set
                orders_numbers_array_agg = array_append(
                    orders_numbers_array_agg,
                    new.doc_number
                ),
                orders_numbers = array_to_string(
                    array_append(
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
after insert or update of doc_date, doc_number, id_client or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();