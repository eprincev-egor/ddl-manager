create or replace function cache000_client_for_orders_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
            100 * new.profit / client.total_profit as percent_of_client_profit
    from companies as client
    where
        client.id = new.id_client
    into new_totals;


    new.percent_of_client_profit = new_totals.percent_of_client_profit;


    return new;
end
$body$
language plpgsql;

create trigger cache000_client_for_orders_bef_ins
before insert
on public.orders
for each row
execute procedure cache000_client_for_orders_bef_ins();