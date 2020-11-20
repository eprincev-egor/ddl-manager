cache totals for companies (
    select
        string_agg(distinct orders.doc_number, ', ') as orders_numbers
    
    from orders
    where
        companies.id = any( orders.companies_ids ) and
        orders.deleted = 0
)