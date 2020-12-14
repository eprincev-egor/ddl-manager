cache totals for companies (
    select 
        sum( orders.profit ) filter(where
            order_type.name = 'LTL'
        ) as ltl_profit,
        
        sum( orders.profit ) filter(where
            order_type.name = 'FTL'
        ) as ftl_orders_profit

    from orders

    left join order_type on
        order_type.id = orders.id_order_type

    where
        orders.id_client = companies.id
)