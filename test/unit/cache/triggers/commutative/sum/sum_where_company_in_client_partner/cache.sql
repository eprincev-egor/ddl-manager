cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        companies.id in (orders.id_client, orders.id_partner)
)