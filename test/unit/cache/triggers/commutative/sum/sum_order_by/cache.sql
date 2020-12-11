cache totals for companies (
    select sum( orders.profit order by orders.doc_date ) as orders_total
    from orders
    where
        orders.id_client = companies.id
)