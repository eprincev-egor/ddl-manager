cache totals for orders (
    select
        orders.profit > 100 as profit_100,
        orders.profit > 200 as profit_200
)