cache invoices for orders (
    select sum( invoice.profit ) as invoices_profit
    from invoice
    
    left join invoice_type on
        invoice_type.id = invoice.id_invoice_type

    where
        invoice.orders_ids @> array[ orders.id ]::bigint[] and
        invoice.deleted = 0 and
        invoice_type.code in ('incoming', 'outgoung')
)