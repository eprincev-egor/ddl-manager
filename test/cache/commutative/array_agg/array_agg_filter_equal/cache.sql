cache totals for companies (
    select
        array_agg(orders.id) filter(where
            orders.id_order_type = 1
        ) as fcl_orders_ids,

        array_agg(orders.id) filter(where
            orders.id_order_type = 2
        ) as ltl_orders_ids
    from orders
    where
        orders.id_client = companies.id
)