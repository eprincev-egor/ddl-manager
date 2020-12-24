cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        companies.id = any( orders.clients_ids || orders.partners_ids ) and
        orders.deleted = 0
)