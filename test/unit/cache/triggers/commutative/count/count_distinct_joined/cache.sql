cache totals for companies (
    select
        count( distinct order_type.name ) as orders_count

    from orders
    
    left join order_type on
        order_type.id = orders.id_order_type

    where
        orders.id_client = companies.id
)