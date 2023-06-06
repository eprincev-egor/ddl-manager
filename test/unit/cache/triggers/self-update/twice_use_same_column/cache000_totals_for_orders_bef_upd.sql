create or replace function cache000_totals_for_orders_bef_upd()
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

create trigger cache000_totals_for_orders_bef_upd
before update of profit
on public.orders
for each row
execute procedure cache000_totals_for_orders_bef_upd();