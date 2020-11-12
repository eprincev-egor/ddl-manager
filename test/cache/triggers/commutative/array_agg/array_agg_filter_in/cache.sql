cache totals for companies (
    select
        array_agg(orders.id) filter(where
            orders.id_order_type in (1, 2, 3)
        ) as general_orders_ids,

        array_agg(orders.id) as all_orders_ids
    from orders
    where
        orders.id_client = companies.id
)