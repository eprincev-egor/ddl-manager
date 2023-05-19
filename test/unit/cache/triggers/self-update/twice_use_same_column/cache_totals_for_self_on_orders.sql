create or replace function cache_totals_for_self_on_orders()
returns trigger as $body$
declare new_totals record;
begin

    if new.profit is not distinct from old.profit then
        return new;
    end if;


    select
        new.profit > 100 as profit_100,
        new.profit > 200 as profit_200
    into new_totals;

    if
        new_totals.profit_100 is distinct from new.profit_100
        or
        new_totals.profit_200 is distinct from new.profit_200
    then

        update orders set
            profit_100 = new_totals.profit_100,
            profit_200 = new_totals.profit_200
        where
            public.orders.id = new.id;

    end if;

    return new;
end
$body$
language plpgsql;

create trigger cache_totals_for_self_on_orders
after update of profit
on public.orders
for each row
execute procedure cache_totals_for_self_on_orders();