cache totals for companies (
    select min( orders.id ) as min_order_id
    from orders
    where
        orders.id_client = companies.id
)