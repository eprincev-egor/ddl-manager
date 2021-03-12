create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare inserted_companies_ids integer[];
declare deleted_companies_ids integer[];
begin

    if TG_OP = 'DELETE' then

        if
            old.companies_ids is not null
            and
            old.deleted = 0
        then
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
                companies.id = any (old.companies_ids);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            cm_equal_arrays(new.companies_ids, old.companies_ids)
            and
            new.deleted is not distinct from old.deleted
            and
            new.doc_number is not distinct from old.doc_number
        then
            return new;
        end if;

        inserted_companies_ids = cm_get_inserted_elements(old.companies_ids, new.companies_ids);
        deleted_companies_ids = cm_get_deleted_elements(old.companies_ids, new.companies_ids);

        if
            cm_equal_arrays(new.companies_ids, old.companies_ids)
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.companies_ids is null
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
                companies.id = any (new.companies_ids);

            return new;
        end if;

        if cm_equal_arrays(old.companies_ids, new.companies_ids) then
            inserted_companies_ids = new.companies_ids;
            deleted_companies_ids = old.companies_ids;
        end if;

        if
            deleted_companies_ids is not null
            and
            old.deleted = 0
        then
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
                companies.id = any (deleted_companies_ids);
        end if;

        if
            inserted_companies_ids is not null
            and
            new.deleted = 0
        then
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
                companies.id = any (inserted_companies_ids);
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.companies_ids is not null
            and
            new.deleted = 0
        then
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
                companies.id = any (new.companies_ids);
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of companies_ids, deleted, doc_number or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();