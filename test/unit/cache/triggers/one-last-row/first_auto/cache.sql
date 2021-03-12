cache first_auto for public.order (
    select
        operations.doc_number as first_auto_number,
        operations.incoming_date as first_incoming_date

    from operations
    where
        operations.id_order = public.order.id
        and
        operations.type = 'auto'
        and
        operations.deleted = 0

    order by operations.id asc
    limit 1
)