cache is_3pl_shipped for supply_order_weight_position as position (
    select
        coalesce(
            bool_or(link.actual_netto_or_pcs is not null),
            false
        ) as is_3pl_shipped

    from supply_order_position_unit_link as link
    where
        link.id_position = position.id
)