cache totals for companies (
    select
        count(*) filter(
            where
                orders.id_order_type in (1,2,3)
        ) as orders_count

    from orders
    where
        orders.id_client = companies.id
)