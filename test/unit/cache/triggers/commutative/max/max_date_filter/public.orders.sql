create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            (
                old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                or
                old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
            )
        then
            update companies set
                max_general_order_date_order_date = case
                    when
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_general_order_date_order_date,
                            old.order_date
                        )
                    else
                        max_general_order_date_order_date
                end,
                max_general_order_date_id_order_type = case
                    when
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_general_order_date_id_order_type,
                            old.id_order_type
                        )
                    else
                        max_general_order_date_id_order_type
                end,
                max_general_order_date = case
                    when
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date) filter (where orders.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))

                            from unnest(
                                cm_array_remove_one_element(
                                    max_general_order_date_order_date,
                                    old.order_date
                                ),
                                cm_array_remove_one_element(
                                    max_general_order_date_id_order_type,
                                    old.id_order_type
                                )
                            ) as item(order_date, id_order_type)
                        )
                    else
                        max_general_order_date
                end,
                max_combiner_order_date_order_date = case
                    when
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_combiner_order_date_order_date,
                            old.order_date
                        )
                    else
                        max_combiner_order_date_order_date
                end,
                max_combiner_order_date_id_order_type = case
                    when
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_combiner_order_date_id_order_type,
                            old.id_order_type
                        )
                    else
                        max_combiner_order_date_id_order_type
                end,
                max_combiner_order_date = case
                    when
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date) filter (where orders.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))

                            from unnest(
                                cm_array_remove_one_element(
                                    max_combiner_order_date_order_date,
                                    old.order_date
                                ),
                                cm_array_remove_one_element(
                                    max_combiner_order_date_id_order_type,
                                    old.id_order_type
                                )
                            ) as item(order_date, id_order_type)
                        )
                    else
                        max_combiner_order_date
                end
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.id_order_type is not distinct from old.id_order_type
            and
            new.order_date is not distinct from old.order_date
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                max_general_order_date_order_date = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                        and
                        not(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))
                    then
                        array_append(
                            max_general_order_date_order_date,
                            new.order_date
                        )
                    when
                        not(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))
                        and
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_general_order_date_order_date,
                            old.order_date
                        )
                    else
                        array_append(
                            cm_array_remove_one_element(
                                max_general_order_date_order_date,
                                old.order_date
                            ),
                            new.order_date
                        )
                end,
                max_general_order_date_id_order_type = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                        and
                        not(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))
                    then
                        array_append(
                            max_general_order_date_id_order_type,
                            new.id_order_type
                        )
                    when
                        not(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))
                        and
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_general_order_date_id_order_type,
                            old.id_order_type
                        )
                    else
                        array_append(
                            cm_array_remove_one_element(
                                max_general_order_date_id_order_type,
                                old.id_order_type
                            ),
                            new.id_order_type
                        )
                end,
                max_general_order_date = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                        and
                        not(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))
                    then
                        greatest(
                            max_general_order_date,
                            new.order_date
                        )
                    when
                        not(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))
                        and
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date) filter (where orders.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))

                            from unnest(
                                cm_array_remove_one_element(
                                    max_general_order_date_order_date,
                                    old.order_date
                                ),
                                cm_array_remove_one_element(
                                    max_general_order_date_id_order_type,
                                    old.id_order_type
                                )
                            ) as item(order_date, id_order_type)
                        )
                    else
                        (
                            select
                                max(item.order_date) filter (where orders.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        max_general_order_date_order_date,
                                        old.order_date
                                    ),
                                    new.order_date
                                ),
                                array_append(
                                    cm_array_remove_one_element(
                                        max_general_order_date_id_order_type,
                                        old.id_order_type
                                    ),
                                    new.id_order_type
                                )
                            ) as item(order_date, id_order_type)
                        )
                end,
                max_combiner_order_date_order_date = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                        and
                        not(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))
                    then
                        array_append(
                            max_combiner_order_date_order_date,
                            new.order_date
                        )
                    when
                        not(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))
                        and
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_combiner_order_date_order_date,
                            old.order_date
                        )
                    else
                        array_append(
                            cm_array_remove_one_element(
                                max_combiner_order_date_order_date,
                                old.order_date
                            ),
                            new.order_date
                        )
                end,
                max_combiner_order_date_id_order_type = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                        and
                        not(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))
                    then
                        array_append(
                            max_combiner_order_date_id_order_type,
                            new.id_order_type
                        )
                    when
                        not(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))
                        and
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_combiner_order_date_id_order_type,
                            old.id_order_type
                        )
                    else
                        array_append(
                            cm_array_remove_one_element(
                                max_combiner_order_date_id_order_type,
                                old.id_order_type
                            ),
                            new.id_order_type
                        )
                end,
                max_combiner_order_date = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                        and
                        not(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))
                    then
                        greatest(
                            max_combiner_order_date,
                            new.order_date
                        )
                    when
                        not(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))
                        and
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date) filter (where orders.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))

                            from unnest(
                                cm_array_remove_one_element(
                                    max_combiner_order_date_order_date,
                                    old.order_date
                                ),
                                cm_array_remove_one_element(
                                    max_combiner_order_date_id_order_type,
                                    old.id_order_type
                                )
                            ) as item(order_date, id_order_type)
                        )
                    else
                        (
                            select
                                max(item.order_date) filter (where orders.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        max_combiner_order_date_order_date,
                                        old.order_date
                                    ),
                                    new.order_date
                                ),
                                array_append(
                                    cm_array_remove_one_element(
                                        max_combiner_order_date_id_order_type,
                                        old.id_order_type
                                    ),
                                    new.id_order_type
                                )
                            ) as item(order_date, id_order_type)
                        )
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            (
                old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                or
                old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
            )
        then
            update companies set
                max_general_order_date_order_date = case
                    when
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_general_order_date_order_date,
                            old.order_date
                        )
                    else
                        max_general_order_date_order_date
                end,
                max_general_order_date_id_order_type = case
                    when
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_general_order_date_id_order_type,
                            old.id_order_type
                        )
                    else
                        max_general_order_date_id_order_type
                end,
                max_general_order_date = case
                    when
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date) filter (where orders.id_order_type = any (array[1, 2, 3, 4] :: bigint[]))

                            from unnest(
                                cm_array_remove_one_element(
                                    max_general_order_date_order_date,
                                    old.order_date
                                ),
                                cm_array_remove_one_element(
                                    max_general_order_date_id_order_type,
                                    old.id_order_type
                                )
                            ) as item(order_date, id_order_type)
                        )
                    else
                        max_general_order_date
                end,
                max_combiner_order_date_order_date = case
                    when
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_combiner_order_date_order_date,
                            old.order_date
                        )
                    else
                        max_combiner_order_date_order_date
                end,
                max_combiner_order_date_id_order_type = case
                    when
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_combiner_order_date_id_order_type,
                            old.id_order_type
                        )
                    else
                        max_combiner_order_date_id_order_type
                end,
                max_combiner_order_date = case
                    when
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date) filter (where orders.id_order_type = any (array[5, 6, 7, 8] :: bigint[]))

                            from unnest(
                                cm_array_remove_one_element(
                                    max_combiner_order_date_order_date,
                                    old.order_date
                                ),
                                cm_array_remove_one_element(
                                    max_combiner_order_date_id_order_type,
                                    old.id_order_type
                                )
                            ) as item(order_date, id_order_type)
                        )
                    else
                        max_combiner_order_date
                end
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            (
                new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                or
                new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
            )
        then
            update companies set
                max_general_order_date_order_date = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        array_append(
                            max_general_order_date_order_date,
                            new.order_date
                        )
                    else
                        max_general_order_date_order_date
                end,
                max_general_order_date_id_order_type = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        array_append(
                            max_general_order_date_id_order_type,
                            new.id_order_type
                        )
                    else
                        max_general_order_date_id_order_type
                end,
                max_general_order_date = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        greatest(
                            max_general_order_date,
                            new.order_date
                        )
                    else
                        max_general_order_date
                end,
                max_combiner_order_date_order_date = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        array_append(
                            max_combiner_order_date_order_date,
                            new.order_date
                        )
                    else
                        max_combiner_order_date_order_date
                end,
                max_combiner_order_date_id_order_type = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        array_append(
                            max_combiner_order_date_id_order_type,
                            new.id_order_type
                        )
                    else
                        max_combiner_order_date_id_order_type
                end,
                max_combiner_order_date = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        greatest(
                            max_combiner_order_date,
                            new.order_date
                        )
                    else
                        max_combiner_order_date
                end
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_client is not null
            and
            (
                new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                or
                new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
            )
        then
            update companies set
                max_general_order_date_order_date = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        array_append(
                            max_general_order_date_order_date,
                            new.order_date
                        )
                    else
                        max_general_order_date_order_date
                end,
                max_general_order_date_id_order_type = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        array_append(
                            max_general_order_date_id_order_type,
                            new.id_order_type
                        )
                    else
                        max_general_order_date_id_order_type
                end,
                max_general_order_date = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        greatest(
                            max_general_order_date,
                            new.order_date
                        )
                    else
                        max_general_order_date
                end,
                max_combiner_order_date_order_date = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        array_append(
                            max_combiner_order_date_order_date,
                            new.order_date
                        )
                    else
                        max_combiner_order_date_order_date
                end,
                max_combiner_order_date_id_order_type = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        array_append(
                            max_combiner_order_date_id_order_type,
                            new.id_order_type
                        )
                    else
                        max_combiner_order_date_id_order_type
                end,
                max_combiner_order_date = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        greatest(
                            max_combiner_order_date,
                            new.order_date
                        )
                    else
                        max_combiner_order_date
                end
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, id_order_type, order_date or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();