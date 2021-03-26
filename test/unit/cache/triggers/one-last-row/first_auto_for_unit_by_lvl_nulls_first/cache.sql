cache first_auto for units (
    select
        first_auto.incoming_date as first_auto_incoming_date,
        first_auto.outgoing_date as first_auto_outgoing_date

    from operations as first_auto
    where
        first_auto.units_ids && array[ units.id ]::bigint[] and
        first_auto.type = 'auto' and
        first_auto.deleted = 0

    order by
        first_auto.lvl asc nulls first
    limit 1
)