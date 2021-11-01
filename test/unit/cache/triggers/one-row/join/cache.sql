cache orders for orders_positions as positions (
    select
        coalesce(country_start.has_surveyor_inspection, 0) as has_surveyor_inspection

    from orders

    left join countries as country_start on
        country_start.id = orders.id_country_start

    where
        orders.id = positions.id_supply_order
        and orders.deleted = 0
)
