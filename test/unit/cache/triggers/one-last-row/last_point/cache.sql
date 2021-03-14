cache last_point for operations (
    select
        arrival_points.actual_date as last_point_actual_date,
        arrival_points.expected_date as last_point_expected_date,
        arrival_points.id_point as last_point_id_point

    from arrival_points
    where
        arrival_points.id_operation = operations.id

    order by
        sort desc
    limit 1
)