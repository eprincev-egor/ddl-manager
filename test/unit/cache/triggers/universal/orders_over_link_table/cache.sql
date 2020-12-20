cache totals for companies (
    select
        max( orders.order_date ) as max_order_date,
        string_agg( distinct orders.order_number, ', ' ) as orders_numbers
    
    from order_company_link as link
    
    left join orders on
        orders.id = link.id_order
    
    where
        link.id_company = companies.id
)