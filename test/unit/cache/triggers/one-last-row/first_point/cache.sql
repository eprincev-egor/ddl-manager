cache first_point for operations (
    select
        first_point.actual_date as first_point_actual_date,
        first_point.expected_date as first_point_expected_date,
        first_point.id_point as first_point_id_point

    from arrival_points as first_point
    where
        first_point.id_operation = operations.id and
        first_point.deleted = 0

    order by
        sort asc
    limit 1
)