cache totals for companies (
    select
        max( orders.order_date ) filter (where 
            orders.id_order_type = any( array[1,2,3,4]::bigint[] )
        ) as max_general_order_date,

        max( orders.order_date ) filter (where 
            orders.id_order_type = any( array[5,6,7,8]::bigint[] )
        ) as max_combiner_order_date

    from orders
    where
        orders.id_client = companies.id
)