create or replace function cache_order_country_for_self_on_invoice()
returns trigger as $body$
begin
    if TG_OP = 'INSERT' then
        if new.orders_ids is null then
            return new;
        end if;
    end if;

    if TG_OP = 'UPDATE' then
        if new.orders_ids is not distinct from old.orders_ids then
            return new;
        end if;
    end if;


    update invoice set
        (
            order_countries_name,
            order_countries
        ) = (
            select
                array_agg(country.name) as order_countries_name,
                string_agg(distinct country.name, ', ') as order_countries

            from orders

            left join country on
                country.id = orders.id_country

            where
                orders.id = any (invoice.orders_ids)
    and
    (
        orders.id_order_type = 1
        or
        orders.id_order_type = 2
    )
        )
    where
        public.invoice.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_order_country_for_self_on_invoice
after insert or update of orders_ids
on public.invoice
for each row
execute procedure cache_order_country_for_self_on_invoice();