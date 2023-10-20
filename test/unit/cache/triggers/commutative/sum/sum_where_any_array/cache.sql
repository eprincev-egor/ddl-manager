cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        orders.id = any( companies.orders_ids ) and
        orders.deleted = 0
)