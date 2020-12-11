cache totals for companies (
    select min( orders.order_date order by orders.dt_create ) as min_order_date
    from orders
    where
        orders.id_client = companies.id
)