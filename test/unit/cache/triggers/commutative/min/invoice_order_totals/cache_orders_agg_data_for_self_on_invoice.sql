create or replace function cache_orders_agg_data_for_self_on_invoice()
returns trigger as $body$
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


    update invoice set
        (
            order_some_date_some_date,
            order_some_date
        ) = (
            select
                array_agg(orders.some_date) as order_some_date_some_date,
                min(orders.some_date) as order_some_date

            from invoice_positions

            left join public.order as orders on
                orders.id = invoice_positions.id_order

            where
                invoice.id_invoice_type = 4
    and
    invoice_positions.id_invoice = invoice.id
        )
    where
        public.invoice.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_orders_agg_data_for_self_on_invoice
after update of id_invoice_type
on public.invoice
for each row
execute procedure cache_orders_agg_data_for_self_on_invoice();