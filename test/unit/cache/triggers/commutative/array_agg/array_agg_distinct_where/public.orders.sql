create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            old.order_date is not null
        then
            update companies set
                orders_dates_order_date = cm_array_remove_one_element(
                    orders_dates_order_date,
                    old.order_date
                ),
                orders_dates = (
                    select
                        array_agg(distinct item.order_date) filter (where orders.order_date is not null)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_dates_order_date,
                            old.order_date
                        )
                    ) as item(order_date)
                )
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.order_date is not distinct from old.order_date
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                orders_dates_order_date = case
                    when
                        new.order_date is not null
                        and
                        not(old.order_date is not null)
                    then
                        array_append(
                            orders_dates_order_date,
                            new.order_date
                        )
                    when
                        not(new.order_date is not null)
                        and
                        old.order_date is not null
                    then
                        cm_array_remove_one_element(
                            orders_dates_order_date,
                            old.order_date
                        )
                    else
                        array_append(
                            cm_array_remove_one_element(
                                orders_dates_order_date,
                                old.order_date
                            ),
                            new.order_date
                        )
                end,
                orders_dates = case
                    when
                        new.order_date is not null
                        and
                        not(old.order_date is not null)
                    then
                        (
                            select
                                array_agg(distinct item.order_date) filter (where orders.order_date is not null)

                            from unnest(
                                array_append(
                                    orders_dates_order_date,
                                    new.order_date
                                )
                            ) as item(order_date)
                        )
                    when
                        not(new.order_date is not null)
                        and
                        old.order_date is not null
                    then
                        (
                            select
                                array_agg(distinct item.order_date) filter (where orders.order_date is not null)

                            from unnest(
                                cm_array_remove_one_element(
                                    orders_dates_order_date,
                                    old.order_date
                                )
                            ) as item(order_date)
                        )
                    else
                        (
                            select
                                array_agg(distinct item.order_date) filter (where orders.order_date is not null)

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        orders_dates_order_date,
                                        old.order_date
                                    ),
                                    new.order_date
                                )
                            ) as item(order_date)
                        )
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            old.order_date is not null
        then
            update companies set
                orders_dates_order_date = cm_array_remove_one_element(
                    orders_dates_order_date,
                    old.order_date
                ),
                orders_dates = (
                    select
                        array_agg(distinct item.order_date) filter (where orders.order_date is not null)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_dates_order_date,
                            old.order_date
                        )
                    ) as item(order_date)
                )
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            new.order_date is not null
        then
            update companies set
                orders_dates_order_date = array_append(
                    orders_dates_order_date,
                    new.order_date
                ),
                orders_dates = (
                    select
                        array_agg(distinct item.order_date) filter (where orders.order_date is not null)

                    from unnest(
                        array_append(
                            orders_dates_order_date,
                            new.order_date
                        )
                    ) as item(order_date)
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
            new.order_date is not null
        then
            update companies set
                orders_dates_order_date = array_append(
                    orders_dates_order_date,
                    new.order_date
                ),
                orders_dates = (
                    select
                        array_agg(distinct item.order_date) filter (where orders.order_date is not null)

                    from unnest(
                        array_append(
                            orders_dates_order_date,
                            new.order_date
                        )
                    ) as item(order_date)
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
after insert or update of id_client, order_date or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();