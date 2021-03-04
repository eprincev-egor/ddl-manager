cache sum_without_insert for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        orders.id_client = companies.id
)
without insert case on orders