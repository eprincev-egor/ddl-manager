create or replace function cache_orders_agg_data_for_invoice_on_order()
returns trigger as $body$
declare new_row record;
declare old_row record;
declare return_row record;
begin
    if TG_OP = 'DELETE' then
        return_row = old;
    else
        return_row = new;
    end if;

    new_row = return_row;
    old_row = return_row;

    if TG_OP in ('INSERT', 'UPDATE') then
        new_row = new;
    end if;
    if TG_OP in ('UPDATE', 'DELETE') then
        old_row = old;
    end if;

    with
        changed_rows as (
            select old_row.id, old_row.some_date
            union
            select new_row.id, new_row.some_date
        )
    update invoice set
        (
            order_some_date,
            __orders_agg_data_json__
        ) = (
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
                invoice.id_invoice_type = 4
                and
                invoice_positions.id_invoice = invoice.id
        )
    from changed_rows, invoice_positions
    where
        invoice.id_invoice_type = 4
        and
        invoice_positions.id_invoice = invoice.id
        and
        changed_rows.id = invoice_positions.id_order;

    return return_row;
end
$body$
language plpgsql;

create trigger cache_orders_agg_data_for_invoice_on_order
after insert or update of some_date or delete
on public.order
for each row
execute procedure cache_orders_agg_data_for_invoice_on_order();