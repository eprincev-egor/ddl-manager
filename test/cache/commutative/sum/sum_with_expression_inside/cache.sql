cache totals for companies (
    select
        sum( (orders.debet - orders.credit) * orders.quantity ) as orders_total
    from orders
    where
        orders.id_client = companies.id
)