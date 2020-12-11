cache totals for companies (
    select count( distinct orders.id_partner ) as orders_count
    from orders
    where
        orders.id_client = companies.id
)