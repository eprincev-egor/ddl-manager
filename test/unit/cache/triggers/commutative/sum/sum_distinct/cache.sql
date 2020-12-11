cache totals for companies (
    select sum( distinct orders.profit ) as orders_total
    from orders
    where
        orders.id_client = companies.id
)