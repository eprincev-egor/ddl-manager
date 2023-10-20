create or replace function cache_client_for_orders_on_companies()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        update orders set
            percent_of_client_profit = 100 * orders.profit / (null::numeric)
        where
            old.id = orders.id_client
            and
            orders.percent_of_client_profit is distinct from (100 * orders.profit / (null::numeric));

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.total_profit is not distinct from old.total_profit then
            return new;
        end if;

        update orders set
            percent_of_client_profit = 100 * orders.profit / new.total_profit
        where
            new.id = orders.id_client
            and
            orders.percent_of_client_profit is distinct from (100 * orders.profit / new.total_profit);

        return new;
    end if;

    if TG_OP = 'INSERT' then
        update orders set
            percent_of_client_profit = 100 * orders.profit / new.total_profit
        where
            new.id = orders.id_client
            and
            orders.percent_of_client_profit is distinct from (100 * orders.profit / new.total_profit);

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_client_for_orders_on_companies
after insert or update of total_profit or delete
on public.companies
for each row
execute procedure cache_client_for_orders_on_companies();