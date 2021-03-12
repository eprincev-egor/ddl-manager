create or replace function cache_totals_for_orders_on_fin_operation()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_order is not null
            and
            old.deleted = 0
            and
            (
                coalesce(old.id_fin_operation_type = 1, false)
                or
                coalesce(old.id_fin_operation_type = 2, false)
            )
        then
            update orders set
                fin_operation_buys = case
                    when
                        old.id_fin_operation_type = 1
                    then
                        fin_operation_buys - coalesce(
                            old.sum * get_curs(
                                old.date,
                                old.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_buys
                end,
                fin_operation_sales = case
                    when
                        old.id_fin_operation_type = 2
                    then
                        fin_operation_sales - coalesce(
                            old.sum * get_curs(
                                old.date,
                                old.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_sales
                end
            where
                old.id_order = orders.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.date is not distinct from old.date
            and
            new.deleted is not distinct from old.deleted
            and
            new.id_currency is not distinct from old.id_currency
            and
            new.id_fin_operation_type is not distinct from old.id_fin_operation_type
            and
            new.id_order is not distinct from old.id_order
            and
            new.sum is not distinct from old.sum
        then
            return new;
        end if;

        if
            new.id_order is not distinct from old.id_order
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_order is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update orders set
                fin_operation_buys = case
                    when
                        new.id_fin_operation_type = 1
                        and
                        not coalesce(old.id_fin_operation_type = 1, false)
                    then
                        fin_operation_buys + coalesce(
                            new.sum * get_curs(
                                new.date,
                                new.id_currency
                            ),
                            0
                        )
                    when
                        not coalesce(new.id_fin_operation_type = 1, false)
                        and
                        old.id_fin_operation_type = 1
                    then
                        fin_operation_buys - coalesce(
                            old.sum * get_curs(
                                old.date,
                                old.id_currency
                            ),
                            0
                        )
                    when
                        new.id_fin_operation_type = 1
                    then
                        fin_operation_buys - coalesce(
                            old.sum * get_curs(
                                old.date,
                                old.id_currency
                            ),
                            0
                        ) + coalesce(
                            new.sum * get_curs(
                                new.date,
                                new.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_buys
                end,
                fin_operation_sales = case
                    when
                        new.id_fin_operation_type = 2
                        and
                        not coalesce(old.id_fin_operation_type = 2, false)
                    then
                        fin_operation_sales + coalesce(
                            new.sum * get_curs(
                                new.date,
                                new.id_currency
                            ),
                            0
                        )
                    when
                        not coalesce(new.id_fin_operation_type = 2, false)
                        and
                        old.id_fin_operation_type = 2
                    then
                        fin_operation_sales - coalesce(
                            old.sum * get_curs(
                                old.date,
                                old.id_currency
                            ),
                            0
                        )
                    when
                        new.id_fin_operation_type = 2
                    then
                        fin_operation_sales - coalesce(
                            old.sum * get_curs(
                                old.date,
                                old.id_currency
                            ),
                            0
                        ) + coalesce(
                            new.sum * get_curs(
                                new.date,
                                new.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_sales
                end
            where
                new.id_order = orders.id;

            return new;
        end if;

        if
            old.id_order is not null
            and
            old.deleted = 0
            and
            (
                coalesce(old.id_fin_operation_type = 1, false)
                or
                coalesce(old.id_fin_operation_type = 2, false)
            )
        then
            update orders set
                fin_operation_buys = case
                    when
                        old.id_fin_operation_type = 1
                    then
                        fin_operation_buys - coalesce(
                            old.sum * get_curs(
                                old.date,
                                old.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_buys
                end,
                fin_operation_sales = case
                    when
                        old.id_fin_operation_type = 2
                    then
                        fin_operation_sales - coalesce(
                            old.sum * get_curs(
                                old.date,
                                old.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_sales
                end
            where
                old.id_order = orders.id;
        end if;

        if
            new.id_order is not null
            and
            new.deleted = 0
            and
            (
                coalesce(new.id_fin_operation_type = 1, false)
                or
                coalesce(new.id_fin_operation_type = 2, false)
            )
        then
            update orders set
                fin_operation_buys = case
                    when
                        new.id_fin_operation_type = 1
                    then
                        fin_operation_buys + coalesce(
                            new.sum * get_curs(
                                new.date,
                                new.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_buys
                end,
                fin_operation_sales = case
                    when
                        new.id_fin_operation_type = 2
                    then
                        fin_operation_sales + coalesce(
                            new.sum * get_curs(
                                new.date,
                                new.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_sales
                end
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_order is not null
            and
            new.deleted = 0
            and
            (
                coalesce(new.id_fin_operation_type = 1, false)
                or
                coalesce(new.id_fin_operation_type = 2, false)
            )
        then
            update orders set
                fin_operation_buys = case
                    when
                        new.id_fin_operation_type = 1
                    then
                        fin_operation_buys + coalesce(
                            new.sum * get_curs(
                                new.date,
                                new.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_buys
                end,
                fin_operation_sales = case
                    when
                        new.id_fin_operation_type = 2
                    then
                        fin_operation_sales + coalesce(
                            new.sum * get_curs(
                                new.date,
                                new.id_currency
                            ),
                            0
                        )
                    else
                        fin_operation_sales
                end
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_fin_operation
after insert or update of date, deleted, id_currency, id_fin_operation_type, id_order, sum or delete
on public.fin_operation
for each row
execute procedure cache_totals_for_orders_on_fin_operation();