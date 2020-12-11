cache totals for companies (
    select max( distinct orders.order_date ) as max_order_date
    from orders
    where
        orders.id_client = companies.id
)