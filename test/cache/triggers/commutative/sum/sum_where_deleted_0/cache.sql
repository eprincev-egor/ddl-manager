cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        orders.id_client = companies.id and
        orders.deleted = 0
)