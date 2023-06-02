create or replace function cache_totals_for_self_on_orders()
returns trigger as $body$
declare new_totals record;
begin

    if new.profit is not distinct from old.profit then
        return new;
    end if;


    new.profit_100 = new.profit > 100;
    new.profit_200 = new.profit > 200;

    return new;
end
$body$
language plpgsql;

create trigger cache_totals_for_self_on_orders
before update of profit
on public.orders
for each row
execute procedure cache_totals_for_self_on_orders();