cache totals for companies (
    select
        array_agg(
            orders.order_date
            order by greatest(orders.order_date, orders.archive_date)
        ) as orders_dates
    from orders
    where
        orders.id_client = companies.id
)