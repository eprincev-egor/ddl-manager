cache totals for companies (
    select 

        string_agg(country_from.name, ', ') as from_countries,
        string_agg(country_to.name, ', ') as to_countries

    from orders
    
    left join countries as country_from on
        country_from.id = orders.id_country_from
    
    left join countries as country_to on
        orders.id_country_to = country_to.id
    
    where
        orders.id_client = companies.id
)