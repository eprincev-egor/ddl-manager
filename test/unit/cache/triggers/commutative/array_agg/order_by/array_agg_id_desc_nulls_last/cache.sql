cache totals for companies (
    select
        array_agg(
            orders.id
            order by 
                orders.id desc 
                    nulls last
        ) as orders_ids
    from orders
    where
        orders.id_client = companies.id
)