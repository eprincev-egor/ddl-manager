create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare old_order_type_name text;
declare new_order_type_name text;
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            if old.id_order_type is not null then
                old_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = old.id_order_type
                );
            end if;

            if
                coalesce(old_order_type_name = 'LTL', false)
                or
                coalesce(old_order_type_name = 'FTL', false)
            then
                update companies set
                    ltl_profit = case
                        when
                            old_order_type_name = 'LTL'
                        then
                            ltl_profit - coalesce(old.profit, 0)
                        else
                            ltl_profit
                    end,
                    ftl_orders_profit = case
                        when
                            old_order_type_name = 'FTL'
                        then
                            ftl_orders_profit - coalesce(old.profit, 0)
                        else
                            ftl_orders_profit
                    end
                where
                    old.id_client = companies.id;
            end if;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.id_order_type is not distinct from old.id_order_type
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if old.id_order_type is not null then
            old_order_type_name = (
                select
                    order_type.name
                from order_type
                where
                    order_type.id = old.id_order_type
            );
        end if;

        if new.id_order_type is not distinct from old.id_order_type then
            new_order_type_name = old_order_type_name;
        else
            if new.id_order_type is not null then
                new_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = new.id_order_type
                );
            end if;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                ltl_profit = case
                    when
                        new_order_type_name = 'LTL'
                        and
                        not coalesce(old_order_type_name = 'LTL', false)
                    then
                        ltl_profit + coalesce(new.profit, 0)
                    when
                        not coalesce(new_order_type_name = 'LTL', false)
                        and
                        old_order_type_name = 'LTL'
                    then
                        ltl_profit - coalesce(old.profit, 0)
                    when
                        new_order_type_name = 'LTL'
                    then
                        ltl_profit - coalesce(old.profit, 0) + coalesce(new.profit, 0)
                    else
                        ltl_profit
                end,
                ftl_orders_profit = case
                    when
                        new_order_type_name = 'FTL'
                        and
                        not coalesce(old_order_type_name = 'FTL', false)
                    then
                        ftl_orders_profit + coalesce(new.profit, 0)
                    when
                        not coalesce(new_order_type_name = 'FTL', false)
                        and
                        old_order_type_name = 'FTL'
                    then
                        ftl_orders_profit - coalesce(old.profit, 0)
                    when
                        new_order_type_name = 'FTL'
                    then
                        ftl_orders_profit - coalesce(old.profit, 0) + coalesce(new.profit, 0)
                    else
                        ftl_orders_profit
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            (
                coalesce(old_order_type_name = 'LTL', false)
                or
                coalesce(old_order_type_name = 'FTL', false)
            )
        then
            update companies set
                ltl_profit = case
                    when
                        old_order_type_name = 'LTL'
                    then
                        ltl_profit - coalesce(old.profit, 0)
                    else
                        ltl_profit
                end,
                ftl_orders_profit = case
                    when
                        old_order_type_name = 'FTL'
                    then
                        ftl_orders_profit - coalesce(old.profit, 0)
                    else
                        ftl_orders_profit
                end
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            (
                coalesce(new_order_type_name = 'LTL', false)
                or
                coalesce(new_order_type_name = 'FTL', false)
            )
        then
            update companies set
                ltl_profit = case
                    when
                        new_order_type_name = 'LTL'
                    then
                        ltl_profit + coalesce(new.profit, 0)
                    else
                        ltl_profit
                end,
                ftl_orders_profit = case
                    when
                        new_order_type_name = 'FTL'
                    then
                        ftl_orders_profit + coalesce(new.profit, 0)
                    else
                        ftl_orders_profit
                end
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            if new.id_order_type is not null then
                new_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = new.id_order_type
                );
            end if;

            if
                coalesce(new_order_type_name = 'LTL', false)
                or
                coalesce(new_order_type_name = 'FTL', false)
            then
                update companies set
                    ltl_profit = case
                        when
                            new_order_type_name = 'LTL'
                        then
                            ltl_profit + coalesce(new.profit, 0)
                        else
                            ltl_profit
                    end,
                    ftl_orders_profit = case
                        when
                            new_order_type_name = 'FTL'
                        then
                            ftl_orders_profit + coalesce(new.profit, 0)
                        else
                            ftl_orders_profit
                    end
                where
                    new.id_client = companies.id;
            end if;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, id_order_type, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();