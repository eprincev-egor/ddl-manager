cache parent_row for operation.operation as child_oper (
    select
        parent_oper.id_order as parent_id_order,
        coalesce(parent_oper.lvl, 0) + 1 as parent_lvl

    from operation.operation as parent_oper
    where
        parent_oper.id = child_oper.id_parent_operation
)