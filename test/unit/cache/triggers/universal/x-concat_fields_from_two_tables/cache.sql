cache orders for invoice (
    select
        sum( orders.profit ) as orders_profit

    from invoice_positions as inv_pos

    left join operation.unit on
        unit.id = inv_pos.id_unit
    
    left join orders on
        orders.id = coalesce(unit.id_order, inv_pos.id_unit)

    where
        inv_pos.id_invoice = invoice.id and
        inv_pos.deleted = 0
)