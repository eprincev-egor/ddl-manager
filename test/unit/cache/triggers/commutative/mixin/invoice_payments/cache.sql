cache payments for invoice (
    select
        string_agg(distinct payment_orders.number, ', ') as payments_no
    from payment_orders
    where
        payment_orders.deleted = 0 and
        payment_orders.id = any (invoice.payments_ids)
)
