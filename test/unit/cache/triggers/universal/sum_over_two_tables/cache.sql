cache totals for orders (
    select
        sum( invoice_positions.cost ) as invoice_positions_cost
    
    from invoice_positions
    
    inner join invoices on
        invoices.id = invoice_positions.invoice_id

    where
        invoices.id_order = orders.id
)