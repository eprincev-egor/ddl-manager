cache order_country for invoice (
    select

        string_agg(
            distinct country.name, ', '
        ) as order_countries

    from orders

    left join country on
        country.id = orders.id_country

    where
        orders.id = ANY( invoice.orders_ids ) and
        (
            orders.id_order_type = 1
            or
            orders.id_order_type = 2
        )
)
without insert case on invoice