cache last_auto_doc for units (
    select
        last_auto_doc.id as id_last_auto_doc

    from operations as last_auto_doc
    where
        last_auto_doc.id_doc_parent_operation = units.id_last_auto
        -- auto
        and last_auto_doc.id_operation_type = 1
        and last_auto_doc.deleted = 0
        and last_auto_doc.units_ids && array[units.id]::bigint[]

    order by
        last_auto_doc.id desc
    limit 1
);