cache first_sea_doc for public.order (
    select
        first_sea_doc.doc_number as first_auto_number,
        first_sea_doc.incoming_date as first_incoming_date

    from operations as first_sea_doc
    where
        first_sea_doc.id_order = public.order.id
        and
        first_sea_doc.type = 'sea'
        and
        first_sea_doc.deleted = 0

    order by first_sea_doc.id asc
    limit 1
)