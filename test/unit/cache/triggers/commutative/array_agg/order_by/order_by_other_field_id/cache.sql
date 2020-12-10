cache totals for companies (
    select
        array_agg(
            orders.doc_number
            order by orders.id
        ) as orders_numbers
    from orders
    where
        orders.id_client = companies.id
)