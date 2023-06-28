create or replace function cache000_orders_for_orders_positions_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
            coalesce(
                country_start.has_surveyor_inspection,
                0
                        ) as has_surveyor_inspection
    from orders

    left join countries as country_start on
        country_start.id = orders.id_country_start
    where
        orders.id = new.id_supply_order
        and
        orders.deleted = 0
    into new_totals;


    new.has_surveyor_inspection = new_totals.has_surveyor_inspection;


    return new;
end
$body$
language plpgsql;

create trigger cache000_orders_for_orders_positions_bef_ins
before insert
on public.orders_positions
for each row
execute procedure cache000_orders_for_orders_positions_bef_ins();