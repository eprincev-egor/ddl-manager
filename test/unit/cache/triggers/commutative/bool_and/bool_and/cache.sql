cache totals for companies (
    select
        bool_and( distinct orders.is_lcl ) as all_orders_is_lcl
    from orders
    where
        orders.id_client = companies.id
)