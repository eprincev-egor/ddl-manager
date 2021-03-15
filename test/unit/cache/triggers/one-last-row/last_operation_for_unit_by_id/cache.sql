cache last_operation for units (
    select
        last_operation.incoming_date as last_operation_incoming_date,
        last_operation.outgoing_date as last_operation_outgoing_date

    from operations as last_operation
    where
        last_operation.units_ids && array[ units.id ]::bigint[]

    order by
        last_operation.id desc
    limit 1
)