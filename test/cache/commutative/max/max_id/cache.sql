cache totals for companies (
    select max( orders.id ) as max_order_id
    from orders
    where
        orders.id_client = companies.id
)