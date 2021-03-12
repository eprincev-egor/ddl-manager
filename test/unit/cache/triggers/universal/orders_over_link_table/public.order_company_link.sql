create or replace function cache_totals_for_companies_on_order_company_link()
returns trigger as $body$
declare old_order_order_date date;
declare old_order_order_number text;
declare new_order_order_date date;
declare new_order_order_number text;
begin

    if TG_OP = 'DELETE' then

        if old.id_company is not null then
            if old.id_order is not null then
                select
                    orders.order_date,
                    orders.order_number
                into
                    old_order_order_date,
                    old_order_order_number
                from orders
                where
                    orders.id = old.id_order;
            end if;

            update companies set
                max_order_date_order_date = cm_array_remove_one_element(
                    max_order_date_order_date,
                    old_order_order_date
                ),
                max_order_date = (
                    select
                        max(item.order_date)

                    from unnest(
                        cm_array_remove_one_element(
                            max_order_date_order_date,
                            old_order_order_date
                        )
                    ) as item(order_date)
                ),
                orders_numbers_order_number = cm_array_remove_one_element(
                    orders_numbers_order_number,
                    old_order_order_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct item.order_number, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_order_number,
                            old_order_order_number
                        )
                    ) as item(order_number)
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

        if old.id_order is not null then
            select
                orders.order_date,
                orders.order_number
            into
                old_order_order_date,
                old_order_order_number
            from orders
            where
                orders.id = old.id_order;
        end if;

        if new.id_order is not distinct from old.id_order then
            new_order_order_date = old_order_order_date;
            new_order_order_number = old_order_order_number;
        else
            if new.id_order is not null then
                select
                    orders.order_date,
                    orders.order_number
                into
                    new_order_order_date,
                    new_order_order_number
                from orders
                where
                    orders.id = new.id_order;
            end if;
        end if;

        if new.id_company is not distinct from old.id_company then
            if new.id_company is null then
                return new;
            end if;

            update companies set
                max_order_date_order_date = array_append(
                    cm_array_remove_one_element(
                        max_order_date_order_date,
                        old_order_order_date
                    ),
                    new_order_order_date
                ),
                max_order_date = (
                    select
                        max(item.order_date)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                max_order_date_order_date,
                                old_order_order_date
                            ),
                            new_order_order_date
                        )
                    ) as item(order_date)
                ),
                orders_numbers_order_number = array_append(
                    cm_array_remove_one_element(
                        orders_numbers_order_number,
                        old_order_order_number
                    ),
                    new_order_order_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct item.order_number, ', ')

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                orders_numbers_order_number,
                                old_order_order_number
                            ),
                            new_order_order_number
                        )
                    ) as item(order_number)
                )
            where
                new.id_company = companies.id;

            return new;
        end if;

        if old.id_company is not null then
            update companies set
                max_order_date_order_date = cm_array_remove_one_element(
                    max_order_date_order_date,
                    old_order_order_date
                ),
                max_order_date = (
                    select
                        max(item.order_date)

                    from unnest(
                        cm_array_remove_one_element(
                            max_order_date_order_date,
                            old_order_order_date
                        )
                    ) as item(order_date)
                ),
                orders_numbers_order_number = cm_array_remove_one_element(
                    orders_numbers_order_number,
                    old_order_order_number
                ),
                orders_numbers = (
                    select
                        string_agg(distinct item.order_number, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            orders_numbers_order_number,
                            old_order_order_number
                        )
                    ) as item(order_number)
                )
            where
                old.id_company = companies.id;
        end if;

        if new.id_company is not null then
            update companies set
                max_order_date_order_date = array_append(
                    max_order_date_order_date,
                    new_order_order_date
                ),
                max_order_date = greatest(
                    max_order_date,
                    new_order_order_date
                ),
                orders_numbers_order_number = array_append(
                    orders_numbers_order_number,
                    new_order_order_number
                ),
                orders_numbers = case
                    when
                        array_position(
                            orders_numbers_order_number,
                            new_order_order_number
                        )
                        is null
                    then
                        coalesce(
                            orders_numbers ||
                            coalesce(
                                ', '
                                || new_order_order_number,
                                ''
                            ),
                            new_order_order_number
                        )
                    else
                        orders_numbers
                end
            where
                new.id_company = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_company is not null then
            if new.id_order is not null then
                select
                    orders.order_date,
                    orders.order_number
                into
                    new_order_order_date,
                    new_order_order_number
                from orders
                where
                    orders.id = new.id_order;
            end if;

            update companies set
                max_order_date_order_date = array_append(
                    max_order_date_order_date,
                    new_order_order_date
                ),
                max_order_date = greatest(
                    max_order_date,
                    new_order_order_date
                ),
                orders_numbers_order_number = array_append(
                    orders_numbers_order_number,
                    new_order_order_number
                ),
                orders_numbers = case
                    when
                        array_position(
                            orders_numbers_order_number,
                            new_order_order_number
                        )
                        is null
                    then
                        coalesce(
                            orders_numbers ||
                            coalesce(
                                ', '
                                || new_order_order_number,
                                ''
                            ),
                            new_order_order_number
                        )
                    else
                        orders_numbers
                end
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