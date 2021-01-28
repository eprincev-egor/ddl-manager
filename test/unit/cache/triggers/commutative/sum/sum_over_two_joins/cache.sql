cache pos_rate for invoice (
    select 
        sum( rate_category.base_cost ) as total_base_cost

    from invoice_positions as inv_pos

    left join operation.rate_expense_type as rate_type on
        rate_type.id = inv_pos.id_operation_rate_expense_type
    
    left join operation.rate_expense_category as rate_category ON
        rate_category.id = rate_type.id_rate_expense_category

    where
        inv_pos.id_invoice = invoice.id AND
        inv_pos.deleted = 0
)