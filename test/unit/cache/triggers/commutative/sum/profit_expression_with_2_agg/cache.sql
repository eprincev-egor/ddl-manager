cache totals for companies (
    select 
        sum( orders.sales ) - sum( orders.buys ) as orders_profit
    from orders
    where
        orders.id_client = companies.id
)