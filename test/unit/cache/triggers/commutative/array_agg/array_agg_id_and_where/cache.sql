cache operation_units for owner.unit (
    select
        array_agg( operation.unit.id ) as operation_units_ids
    from operation.unit
    where
        operation.unit.id_owner_unit = owner.unit.id and
        operation.unit.deleted = 0
)