cache totals for companies (
    select
        string_agg(
            orders.doc_number, ', '
            order by orders.id
        ) as orders_numbers

    from orders
    where
        orders.id_client = companies.id
)