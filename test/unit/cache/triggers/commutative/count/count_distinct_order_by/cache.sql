cache totals for companies (
    select
        count(
            distinct orders.id_partner
            order by
                orders.profit
        ) as orders_count

    from orders
    where
        orders.id_client = companies.id
)