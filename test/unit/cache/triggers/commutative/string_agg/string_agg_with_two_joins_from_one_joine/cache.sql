cache totals for companies (
    select 

        string_agg(countries.code, ', ') as countries_codes,
        string_agg(countries.name, ', ') as countries_names

    from orders
    
    left join countries on
        countries.id = orders.id_country
    
    where
        orders.id_client = companies.id
)