cache totals for companies (
    select
        array_agg(
            orders.order_date
            order by 
                orders.order_date desc 
                    nulls last
        ) as orders_dates
    from orders
    where
        orders.id_client = companies.id
)