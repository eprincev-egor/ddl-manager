cache totals for companies (
    select min( orders.order_date ) as min_order_date
    from orders
    where
        orders.id_client = companies.id
)