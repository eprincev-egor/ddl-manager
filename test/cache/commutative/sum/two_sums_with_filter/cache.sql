cache totals for companies (
    select 
        sum( orders.total ) filter(where
            orders.is_sale
        )
        - 
        sum( orders.total ) filter(where
            orders.is_buy
        )
        as orders_profit

    from orders
    where
        orders.id_client = companies.id
)