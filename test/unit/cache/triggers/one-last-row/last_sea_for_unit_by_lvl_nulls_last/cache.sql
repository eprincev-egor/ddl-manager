cache last_sea for units (
    select
        last_sea.incoming_date as last_sea_incoming_date,
        last_sea.outgoing_date as last_sea_outgoing_date

    from operations as last_sea
    where
        last_sea.units_ids && array[ units.id ]::bigint[] and
        last_sea.type = 'sea' and
        last_sea.deleted = 0

    order by
        last_sea.lvl desc nulls last
    limit 1
)