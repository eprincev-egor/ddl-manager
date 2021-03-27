cache invoices for orders (
    select sum( invoice.profit ) as invoices_profit
    from invoice
    where
        invoice.orders_ids @> array[ orders.id ]::bigint[] and
        invoice.deleted = 0
)