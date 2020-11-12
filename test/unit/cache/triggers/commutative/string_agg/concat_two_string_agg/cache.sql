cache totals for companies (
    select

        string_agg(
            distinct orders.doc_number, ', '
        )
        || ' : ' ||
        string_agg(
            distinct orders.note, ', '
        ) 
        as orders_numbers

    from orders
    where
        orders.id_client = companies.id
)