cache totals for companies (
    select string_agg(orders.doc_number, ';') as orders_numbers
    from orders
    where
        orders.id_client = companies.id
)