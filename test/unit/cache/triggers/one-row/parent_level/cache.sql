cache parent_lvl for operation.operation as child_oper (
    select
        coalesce(parent_oper.lvl, 0) + 1 as lvl

    from operation.operation as parent_oper
    where
        parent_oper.id = child_oper.id_parent_operation
)