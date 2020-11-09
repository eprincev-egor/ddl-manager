cache totals for companies (
    select 
        array_agg(orders.id) as orders_ids,
        count(*) as orders_count
    from orders
    where
        orders.id_client = companies.id
)