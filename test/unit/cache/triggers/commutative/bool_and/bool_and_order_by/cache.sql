cache totals for companies (
    select
        bool_and( orders.is_lcl order by orders.is_lcl ) as all_orders_is_lcl
    from orders
    where
        orders.id_client = companies.id
)