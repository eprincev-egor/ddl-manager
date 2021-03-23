create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                orders_dates_order_date = cm_array_remove_one_element(
                    orders_dates_order_date,
                    old.order_date
                ),
                orders_dates_archive_date = cm_array_remove_one_element(
                    orders_dates_archive_date,
                    old.archive_date
                ),
                orders_dates = (
                    select
                        array_agg(
                            item.order_date
                            order by
                                greatest(
                                    item.order_date,
                                    item.archive_date
                                ) asc nulls last
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            orders_dates_order_date,
                            old.order_date
                        ),
                        cm_array_remove_one_element(
                            orders_dates_archive_date,
                            old.archive_date
                        )
                    ) as item(order_date, archive_date)
                )
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.archive_date is not distinct from old.archive_date
            and
            new.id_client is not distinct from old.id_client
            and
            new.order_date is not distinct from old.order_date
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            if new.id_client is null then
                return new;
            end if;

            update companies set
                orders_dates_order_date = array_append(
                    cm_array_remove_one_element(
                        orders_dates_order_date,
                        old.order_date
                    ),
                    new.order_date
                ),
                orders_dates_archive_date = array_append(
                    cm_array_remove_one_element(
                        orders_dates_archive_date,
                        old.archive_date
                    ),
                    new.archive_date
                ),
                orders_dates = (
                    select
                        array_agg(
                            item.order_date
                            order by
                                greatest(
                                    item.order_date,
                                    item.archive_date
                                ) asc nulls last
                        )

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                orders_dates_order_date,
                                old.order_date
                            ),
                            new.order_date
                        ),
                        array_append(
                            cm_array_remove_one_element(
                                orders_dates_archive_date,
                                old.archive_date
                            ),
                            new.archive_date
                        )
                    ) as item(order_date, archive_date)
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_dates_order_date = cm_array_remove_one_element(
                    orders_dates_order_date,
                    old.order_date
                ),
                orders_dates_archive_date = cm_array_remove_one_element(
                    orders_dates_archive_date,
                    old.archive_date
                ),
                orders_dates = (
                    select
                        array_agg(
                            item.order_date
                            order by
                                greatest(
                                    item.order_date,
                                    item.archive_date
                                ) asc nulls last
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            orders_dates_order_date,
                            old.order_date
                        ),
                        cm_array_remove_one_element(
                            orders_dates_archive_date,
                            old.archive_date
                        )
                    ) as item(order_date, archive_date)
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_dates_order_date = array_append(
                    orders_dates_order_date,
                    new.order_date
                ),
                orders_dates_archive_date = array_append(
                    orders_dates_archive_date,
                    new.archive_date
                ),
                orders_dates = (
                    select
                        array_agg(
                            item.order_date
                            order by
                                greatest(
                                    item.order_date,
                                    item.archive_date
                                ) asc nulls last
                        )

                    from unnest(
                        array_append(
                            orders_dates_order_date,
                            new.order_date
                        ),
                        array_append(
                            orders_dates_archive_date,
                            new.archive_date
                        )
                    ) as item(order_date, archive_date)
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                orders_dates_order_date = array_append(
                    orders_dates_order_date,
                    new.order_date
                ),
                orders_dates_archive_date = array_append(
                    orders_dates_archive_date,
                    new.archive_date
                ),
                orders_dates = (
                    select
                        array_agg(
                            item.order_date
                            order by
                                greatest(
                                    item.order_date,
                                    item.archive_date
                                ) asc nulls last
                        )

                    from unnest(
                        array_append(
                            orders_dates_order_date,
                            new.order_date
                        ),
                        array_append(
                            orders_dates_archive_date,
                            new.archive_date
                        )
                    ) as item(order_date, archive_date)
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
after insert or update of archive_date, id_client, order_date or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();