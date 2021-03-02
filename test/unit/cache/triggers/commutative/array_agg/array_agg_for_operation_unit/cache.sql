cache trains_ids for operation.unit (
    select
        array_agg(distinct link.id_train) as trains_ids
    from train_unit_link as link
    where
        link.id_unit = unit.id and
        link.deleted = 0
)