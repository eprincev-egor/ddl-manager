cache trains for operation.unit (
    select
        string_agg( distinct train.number, ', ' ) as train_numbers

    from train
    where
        train.units_ids && ARRAY[ unit.id ]::bigint[] and
        train.deleted = 0
)