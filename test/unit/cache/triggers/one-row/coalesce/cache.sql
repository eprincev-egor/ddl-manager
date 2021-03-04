cache operation for comments (
    select
        coalesce(
            operation.doc_parent_id_order,
            operation.id_order
        ) as operation_id_order,

        operation.id_operation_type as operation_type_id

    from operation.operation
    where
        operation.id = comments.row_id and
        operation.deleted = 0 and
        comments.query_name = 'OPERATION'
)