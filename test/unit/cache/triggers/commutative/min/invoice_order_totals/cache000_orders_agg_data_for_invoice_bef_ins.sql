create or replace function cache000_orders_agg_data_for_invoice_bef_ins()
returns trigger as $body$
declare new_totals record;
begin


    if not coalesce(new.id_invoice_type = 4, false) then
        return new;
    end if;


    select
            min(orders.some_date) as order_some_date,
            ('{' || string_agg(
                                            '"' || public.invoice_positions.id::text || '":' || jsonb_build_object(
                        'id', public.invoice_positions.id,'id_invoice', public.invoice_positions.id_invoice,'id_order', public.invoice_positions.id_order
                    )::text,
                                            ','
                                        ) || '}')
            ::
            jsonb as __orders_agg_data_json__
    from invoice_positions

    left join public.order as orders on
        orders.id = invoice_positions.id_order
    where
        new.id_invoice_type = 4
        and
        invoice_positions.id_invoice = new.id
    into new_totals;


    new.order_some_date = new_totals.order_some_date;
    new.__orders_agg_data_json__ = new_totals.__orders_agg_data_json__;


    return new;
end
$body$
language plpgsql;

create trigger cache000_orders_agg_data_for_invoice_bef_ins
before insert
on public.invoice
for each row
execute procedure cache000_orders_agg_data_for_invoice_bef_ins();