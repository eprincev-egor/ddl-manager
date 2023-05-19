create or replace function cache001_orders_profit_200_bef_ins()
returns trigger as $body$
begin
    new.profit_200 = new.profit > 200;

    return new;
end
$body$
language plpgsql;

create trigger cache001_orders_profit_200_bef_ins
before insert
on public.orders
for each row
execute procedure cache001_orders_profit_200_bef_ins();