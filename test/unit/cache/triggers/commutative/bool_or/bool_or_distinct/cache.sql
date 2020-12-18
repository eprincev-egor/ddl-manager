cache totals for companies (
    select
        bool_or( orders.is_lcl ) as has_lcl_order
    from orders
    where
        orders.id_client = companies.id
)