create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            (
                coalesce(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                or
                coalesce(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
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
                max_general_order_date = case
                    when
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date)

                            from unnest(
                                cm_array_remove_one_element(
                                    max_general_order_date_order_date,
                                    old.order_date
                                )
                            ) as item(order_date)
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
                max_combiner_order_date = case
                    when
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date)

                            from unnest(
                                cm_array_remove_one_element(
                                    max_combiner_order_date_order_date,
                                    old.order_date
                                )
                            ) as item(order_date)
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
            if new.id_client is null then
                return new;
            end if;

            update companies set
                max_general_order_date_order_date = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                        and
                        not coalesce(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                    then
                        array_append(
                            max_general_order_date_order_date,
                            new.order_date
                        )
                    when
                        not coalesce(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                        and
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_general_order_date_order_date,
                            old.order_date
                        )
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        array_append(
                            cm_array_remove_one_element(
                                max_general_order_date_order_date,
                                old.order_date
                            ),
                            new.order_date
                        )
                    else
                        max_general_order_date_order_date
                end,
                max_general_order_date = case
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                        and
                        not coalesce(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                    then
                        greatest(
                            max_general_order_date,
                            new.order_date
                        )
                    when
                        not coalesce(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                        and
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date)

                            from unnest(
                                cm_array_remove_one_element(
                                    max_general_order_date_order_date,
                                    old.order_date
                                )
                            ) as item(order_date)
                        )
                    when
                        new.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date)

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        max_general_order_date_order_date,
                                        old.order_date
                                    ),
                                    new.order_date
                                )
                            ) as item(order_date)
                        )
                    else
                        max_general_order_date
                end,
                max_combiner_order_date_order_date = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                        and
                        not coalesce(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
                    then
                        array_append(
                            max_combiner_order_date_order_date,
                            new.order_date
                        )
                    when
                        not coalesce(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
                        and
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        cm_array_remove_one_element(
                            max_combiner_order_date_order_date,
                            old.order_date
                        )
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        array_append(
                            cm_array_remove_one_element(
                                max_combiner_order_date_order_date,
                                old.order_date
                            ),
                            new.order_date
                        )
                    else
                        max_combiner_order_date_order_date
                end,
                max_combiner_order_date = case
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                        and
                        not coalesce(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
                    then
                        greatest(
                            max_combiner_order_date,
                            new.order_date
                        )
                    when
                        not coalesce(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
                        and
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date)

                            from unnest(
                                cm_array_remove_one_element(
                                    max_combiner_order_date_order_date,
                                    old.order_date
                                )
                            ) as item(order_date)
                        )
                    when
                        new.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date)

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        max_combiner_order_date_order_date,
                                        old.order_date
                                    ),
                                    new.order_date
                                )
                            ) as item(order_date)
                        )
                    else
                        max_combiner_order_date
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            (
                coalesce(old.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                or
                coalesce(old.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
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
                max_general_order_date = case
                    when
                        old.id_order_type = any (array[1, 2, 3, 4] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date)

                            from unnest(
                                cm_array_remove_one_element(
                                    max_general_order_date_order_date,
                                    old.order_date
                                )
                            ) as item(order_date)
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
                max_combiner_order_date = case
                    when
                        old.id_order_type = any (array[5, 6, 7, 8] :: bigint[])
                    then
                        (
                            select
                                max(item.order_date)

                            from unnest(
                                cm_array_remove_one_element(
                                    max_combiner_order_date_order_date,
                                    old.order_date
                                )
                            ) as item(order_date)
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
                coalesce(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                or
                coalesce(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
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
                coalesce(new.id_order_type = any (array[1, 2, 3, 4] :: bigint[]), false)
                or
                coalesce(new.id_order_type = any (array[5, 6, 7, 8] :: bigint[]), false)
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