cache client for orders (
    select
        100 * orders.profit / client.total_profit as percent_of_client_profit
    from companies as client
    where
        client.id = orders.id_client
)