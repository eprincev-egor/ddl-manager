cache test for public.order (
    select
        case
            when every(public.invoice.payment_date is not null)
            then 1
            else 0
        end
        as all_invoices_has_payment
    from public.invoice
    where
        public.invoice.orders_ids && ARRAY[ public.order.id ]::bigint[]
        and public.invoice.id_invoice_type = 2 -- special type
        and public.invoice.deleted = 0
)