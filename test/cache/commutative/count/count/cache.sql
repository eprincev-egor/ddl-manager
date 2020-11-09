cache totals for companies (
    select count(*) as orders_count
    from orders
    where
        orders.id_client = companies.id
)