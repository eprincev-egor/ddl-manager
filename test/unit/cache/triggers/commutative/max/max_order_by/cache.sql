cache totals for companies (
    select max( orders.order_date order by orders.dt_create ) as max_order_date
    from orders
    where
        orders.id_client = companies.id
)