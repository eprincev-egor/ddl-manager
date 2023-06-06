create or replace function cache000_totals_for_orders_bef_ins()
returns trigger as $body$
begin
    new.profit_100 = new.profit > 100;
    new.profit_200 = new.profit > 200;

    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_orders_bef_ins
before insert
on public.orders
for each row
execute procedure cache000_totals_for_orders_bef_ins();