cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        unknown_func(companies.id, orders.id_client) and
        orders.deleted = 0
)