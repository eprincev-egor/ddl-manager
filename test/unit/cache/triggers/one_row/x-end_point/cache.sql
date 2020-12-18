cache totals for orders (
    select
        end_car_point.expected_date as end_car_point_eta,
        end_car_point.actual_date as end_car_point_ata

    from points as end_car_point
    where
        end_car_point.id_order = orders.id and
        end_car_point.transport_type in ('car', 'truck')

    order by end_car_point.id desc
    limit 1
)