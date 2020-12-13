create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            old.doc_number is not null
        then
            update companies set
                orders_numbers_doc_number = cm_array_remove_one_element(
                    orders_numbers_doc_number,
                    old.doc_number
                ),
                orders_numbers_id = cm_array_remove_one_element(
                    orders_numbers_id,
                    old.id
                ),
                orders_numbers = (
                    select
                        string_agg(
                            item.doc_number,
                            ', '
                            order by
                                item.id asc nulls last
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        ),
                        cm_array_remove_one_element(
                            orders_numbers_id,
                            old.id
                        )
                    ) as item(doc_number, id)
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
                orders_numbers_doc_number = array_append(
                    cm_array_remove_one_element(
                        orders_numbers_doc_number,
                        old.doc_number
                    ),
                    new.doc_number
                ),
                orders_numbers = (
                    select
                        string_agg(
                            item.doc_number,
                            ', '
                            order by
                                item.id asc nulls last
                        )

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                orders_numbers_doc_number,
                                old.doc_number
                            ),
                            new.doc_number
                        ),
                        orders_numbers_id
                    ) as item(doc_number, id)
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
                orders_numbers_doc_number = cm_array_remove_one_element(
                    orders_numbers_doc_number,
                    old.doc_number
                ),
                orders_numbers_id = cm_array_remove_one_element(
                    orders_numbers_id,
                    old.id
                ),
                orders_numbers = (
                    select
                        string_agg(
                            item.doc_number,
                            ', '
                            order by
                                item.id asc nulls last
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        ),
                        cm_array_remove_one_element(
                            orders_numbers_id,
                            old.id
                        )
                    ) as item(doc_number, id)
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
                orders_numbers_doc_number = array_append(
                    orders_numbers_doc_number,
                    new.doc_number
                ),
                orders_numbers_id = array_append(
                    orders_numbers_id,
                    new.id
                ),
                orders_numbers = (
                    select
                        string_agg(
                            item.doc_number,
                            ', '
                            order by
                                item.id asc nulls last
                        )

                    from unnest(
                        array_append(
                            orders_numbers_doc_number,
                            new.doc_number
                        ),
                        array_append(
                            orders_numbers_id,
                            new.id
                        )
                    ) as item(doc_number, id)
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
                orders_numbers_doc_number = array_append(
                    orders_numbers_doc_number,
                    new.doc_number
                ),
                orders_numbers_id = array_append(
                    orders_numbers_id,
                    new.id
                ),
                orders_numbers = (
                    select
                        string_agg(
                            item.doc_number,
                            ', '
                            order by
                                item.id asc nulls last
                        )

                    from unnest(
                        array_append(
                            orders_numbers_doc_number,
                            new.doc_number
                        ),
                        array_append(
                            orders_numbers_id,
                            new.id
                        )
                    ) as item(doc_number, id)
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
after insert or update of doc_number, id_client or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();