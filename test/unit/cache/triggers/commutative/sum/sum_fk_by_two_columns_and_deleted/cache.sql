cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        orders.deleted = 0
        and
        (
            orders.id_client = companies.id
            or
            orders.id_partner = companies.id
        )
)