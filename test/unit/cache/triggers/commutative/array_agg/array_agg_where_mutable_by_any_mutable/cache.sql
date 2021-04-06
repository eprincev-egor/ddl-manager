cache invoices for list_gtd as gtd (
    select
        array_agg( distinct inv_pos.id_invoice ) as by_unit_invoices_ids
    from invoice_positions as inv_pos
    where
        inv_pos.id_operation_unit = any( gtd.operation_units_ids ) and
        inv_pos.deleted = 0
)
