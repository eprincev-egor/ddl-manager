create or replace function cache000_order_country_for_invoice_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
        array_agg(country.name) as order_countries_name,
        string_agg(distinct country.name, ', ') as order_countries
    from orders left join country on
country.id = orders.id_country
    where
        orders.id = any (new.orders_ids)
        and
        (
            orders.id_order_type = 1
            or
            orders.id_order_type = 2
        )
    into new_totals;


    new.order_countries_name = new_totals.order_countries_name;
    new.order_countries = new_totals.order_countries;


    return new;
end
$body$
language plpgsql;

create trigger cache000_order_country_for_invoice_bef_ins
before insert
on public.invoice
for each row
execute procedure cache000_order_country_for_invoice_bef_ins();