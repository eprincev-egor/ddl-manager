cache totals for companies (
    select
        array_agg( distinct orders.order_date ) filter (where
            orders.order_date is not null
        ) as orders_dates

    from orders
    where
        orders.id_client = companies.id
)