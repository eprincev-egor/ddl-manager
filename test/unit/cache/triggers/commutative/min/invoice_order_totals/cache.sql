cache orders_agg_data for invoice (
    select
        min( orders.some_date ) as order_some_date

    from invoice_positions

    left join public.order as orders on 
        orders.id = invoice_positions.id_order

    where
        invoice.id_invoice_type = 4 and
        invoice_positions.id_invoice = invoice.id
)
