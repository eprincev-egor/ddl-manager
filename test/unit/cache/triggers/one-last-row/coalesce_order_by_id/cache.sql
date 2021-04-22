cache border_crossing for public.order as orders (
    select
        border_crossing.id as id_border_crossing,
        coalesce(
            border_crossing.end_expected_date,
            orders.date_delivery
        ) as date_delivery

    from operation.operation as border_crossing
    where
        border_crossing.id_order = orders.id and
        border_crossing.is_border_crossing = 1 and
        border_crossing.id_doc_parent_operation is null and
        border_crossing.deleted = 0

    order by border_crossing.id desc
    limit 1
)