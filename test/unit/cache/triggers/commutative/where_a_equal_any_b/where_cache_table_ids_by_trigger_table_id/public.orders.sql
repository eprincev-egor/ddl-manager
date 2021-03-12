create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.deleted = 0 then
            update companies set
                orders_numbers_doc_number = cm_array_remove_one_element(
                    orders_numbers_doc_number,
                    old.doc_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct item.doc_number, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        )
                    ) as item(doc_number)
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
            if
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

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
                        string_agg(distinct item.doc_number, ', ')

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
            where
                companies.order_ids && ARRAY[ new.id ];

            return new;
        end if;

        if old.deleted = 0 then
            update companies set
                orders_numbers_doc_number = cm_array_remove_one_element(
                    orders_numbers_doc_number,
                    old.doc_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct item.doc_number, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_doc_number,
                            old.doc_number
                        )
                    ) as item(doc_number)
                )
            where
                companies.order_ids && ARRAY[ old.id ];
        end if;

        if new.deleted = 0 then
            update companies set
                orders_numbers_doc_number = array_append(
                    orders_numbers_doc_number,
                    new.doc_number
                ),
                orders_numbers = case
                    when
                        array_position(
                            orders_numbers_doc_number,
                            new.doc_number
                        )
                        is null
                    then
                        coalesce(
                            orders_numbers ||
                            coalesce(
                                ', '
                                || new.doc_number,
                                ''
                            ),
                            new.doc_number
                        )
                    else
                        orders_numbers
                end
            where
                companies.order_ids && ARRAY[ new.id ];
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.deleted = 0 then
            update companies set
                orders_numbers_doc_number = array_append(
                    orders_numbers_doc_number,
                    new.doc_number
                ),
                orders_numbers = case
                    when
                        array_position(
                            orders_numbers_doc_number,
                            new.doc_number
                        )
                        is null
                    then
                        coalesce(
                            orders_numbers ||
                            coalesce(
                                ', '
                                || new.doc_number,
                                ''
                            ),
                            new.doc_number
                        )
                    else
                        orders_numbers
                end
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