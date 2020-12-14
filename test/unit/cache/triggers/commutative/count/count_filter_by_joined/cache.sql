cache totals for companies (
    select
        count(*) filter(
            where
                countries.code = 'RUS'
        ) as orders_count

    from orders

    left join countries on
        countries.id = orders.id_country

    where
        orders.id_client = companies.id
)