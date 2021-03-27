cache totals for gtd (
    select
        count(*) as units_count
    from units
    where
        units.orders_ids && gtd.orders_ids and
        units.deleted = 0
)