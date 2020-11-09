cache totals for companies (
    select 
        count( * ) as orders_count,
        sum( orders.profit ) as orders_total
    from orders
    where
        orders.id_client = companies.id
)