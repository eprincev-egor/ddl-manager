cache totals for companies (
    select
        count(*) filter(
            where
                countries.code = 'RUS'
        ) as rus_orders_count,

        count(*) filter(
            where
                countries.code = 'ENG'
        ) as eng_orders_count

    from orders

    left join countries on
        countries.id = orders.id_country

    where
        orders.id_client = companies.id
)