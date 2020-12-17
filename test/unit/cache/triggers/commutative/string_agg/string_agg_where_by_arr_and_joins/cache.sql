cache totals for companies (
    select 

        string_agg(country.name, ', ') as countries_names

    from orders
    
    left join countries as country on
        country.id = orders.id_country
    
    where
        orders.clients_ids && array[ companies.id ] and
        orders.deleted = 0
)