cache totals for companies (
    select
        array_agg(
            orders.order_date
            order by orders.order_date desc
        ) as orders_dates
    from orders
    where
        orders.id_client = companies.id
)