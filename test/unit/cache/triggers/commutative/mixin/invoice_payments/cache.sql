cache payments for invoice (
    select
        sum(payment_orders.total) as payments_total

    from payment_orders
    where
        payment_orders.deleted = 0 and
        payment_orders.id = any(invoice.payments_ids)
)
