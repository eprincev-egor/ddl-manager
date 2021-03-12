create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                orders_total_profit = cm_array_remove_one_element(
                    orders_total_profit,
                    old.profit
                ),
                orders_total = (
                    select
                        sum(distinct item.profit)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_total_profit,
                            old.profit
                        )
                    ) as item(profit)
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
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            if new.id_client is null then
                return new;
            end if;

            update companies set
                orders_total_profit = array_append(
                    cm_array_remove_one_element(
                        orders_total_profit,
                        old.profit
                    ),
                    new.profit
                ),
                orders_total = (
                    select
                        sum(distinct item.profit)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                orders_total_profit,
                                old.profit
                            ),
                            new.profit
                        )
                    ) as item(profit)
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_total_profit = cm_array_remove_one_element(
                    orders_total_profit,
                    old.profit
                ),
                orders_total = (
                    select
                        sum(distinct item.profit)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_total_profit,
                            old.profit
                        )
                    ) as item(profit)
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_total_profit = array_append(
                    orders_total_profit,
                    new.profit
                ),
                orders_total = (
                    select
                        sum(distinct item.profit)

                    from unnest(
                        array_append(
                            orders_total_profit,
                            new.profit
                        )
                    ) as item(profit)
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                orders_total_profit = array_append(
                    orders_total_profit,
                    new.profit
                ),
                orders_total = (
                    select
                        sum(distinct item.profit)

                    from unnest(
                        array_append(
                            orders_total_profit,
                            new.profit
                        )
                    ) as item(profit)
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
after insert or update of id_client, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();