cache balance for invoice (
    select
        (
            public.invoice.invoice_summ
            - sum( round(inv_pos.total_sum_with_vat_in_curs::numeric, -2 ) )
        ) as balance
    from public.invoice_positions as inv_pos
    where
        inv_pos.id_invoice = public.invoice.id
        and inv_pos.deleted = 0
)