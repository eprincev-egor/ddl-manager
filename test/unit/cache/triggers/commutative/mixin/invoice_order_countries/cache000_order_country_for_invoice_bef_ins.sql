create or replace function cache000_order_country_for_invoice_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
            string_agg(distinct country.name, ', ') as order_countries,
            ('{' || string_agg(
                                            '"' || public.orders.id::text || '":' || jsonb_build_object(
                        'id', public.orders.id,'id_country', public.orders.id_country,'id_order_type', public.orders.id_order_type
                    )::text,
                                            ','
                                        ) || '}')
            ::
            jsonb as __order_country_json__
    from orders

    left join country on
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


    new.order_countries = new_totals.order_countries;
    new.__order_country_json__ = new_totals.__order_country_json__;


    return new;
end
$body$
language plpgsql;

create trigger cache000_order_country_for_invoice_bef_ins
before insert
on public.invoice
for each row
execute procedure cache000_order_country_for_invoice_bef_ins();