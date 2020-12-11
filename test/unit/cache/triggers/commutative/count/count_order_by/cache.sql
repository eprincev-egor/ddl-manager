cache totals for companies (
    select count( orders.profit order by orders.doc_number) as orders_count
    from orders
    where
        orders.id_client = companies.id
)