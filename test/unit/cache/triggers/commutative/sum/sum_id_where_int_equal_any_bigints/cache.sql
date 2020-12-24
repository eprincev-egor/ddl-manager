cache totals for companies (
    select sum( orders.id ) as orders_total
    from orders
    where
        orders.id = any( companies.bigint_orders_ids )
)