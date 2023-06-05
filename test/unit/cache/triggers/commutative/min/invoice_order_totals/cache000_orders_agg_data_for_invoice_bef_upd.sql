create or replace function cache000_orders_agg_data_for_invoice_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if new.id_invoice_type is not distinct from old.id_invoice_type then
        return new;
    end if;

    if
        not coalesce(old.id_invoice_type = 4, false)
        and
        not coalesce(new.id_invoice_type = 4, false)
    then
        return new;
    end if;


    select
        array_agg(orders.some_date) as order_some_date_some_date,
        min(orders.some_date) as order_some_date
    from invoice_positions left join public.order as orders on
orders.id = invoice_positions.id_order
    where
        new.id_invoice_type = 4
        and
        invoice_positions.id_invoice = new.id
    into new_totals;


    new.order_some_date_some_date = new_totals.order_some_date_some_date;
    new.order_some_date = new_totals.order_some_date;


    return new;
end
$body$
language plpgsql;

create trigger cache000_orders_agg_data_for_invoice_bef_upd
before update of id_invoice_type
on public.invoice
for each row
execute procedure cache000_orders_agg_data_for_invoice_bef_upd();