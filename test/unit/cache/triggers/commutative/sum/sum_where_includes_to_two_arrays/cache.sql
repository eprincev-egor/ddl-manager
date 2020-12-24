cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        orders.clients_ids && array[ companies.id ] and
        orders.partners_ids && array[ companies.id ] and
        orders.deleted = 0
)