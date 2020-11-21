cache totals for companies (
    select
        string_agg(distinct orders.doc_number, ', ') as orders_numbers
    
    from orders
    where
        orders.id = any( companies.order_ids ) and
        orders.deleted = 0
)