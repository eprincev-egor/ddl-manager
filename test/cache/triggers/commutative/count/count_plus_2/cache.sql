cache totals for companies (
    select count(*) + 2 as orders_count_2
    from orders
    where
        orders.id_client = companies.id
)