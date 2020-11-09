cache totals for companies (
    select

        array_agg( orders.date ) filter(where
            orders.id_order_type in (1, 2, 3)
        ) as general_orders_dates

    from orders
    where
        orders.id_client = companies.id
)