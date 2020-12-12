cache totals for companies (
    select
        string_agg(
            distinct orders.doc_number, ', '
            order by orders.doc_number
        ) filter (
            where
                orders.profit > 0
        ) as orders_numbers

    from orders
    where
        orders.id_client = companies.id
)