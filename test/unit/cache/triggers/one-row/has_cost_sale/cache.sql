cache has_cost_sale for operation.fin_operation as fin_op (
    select
        (
            operation.operation.cost_sale is not null
            and
            operation.operation.deleted = 0
        ) as is_cost_sale

    from operation.operation
    where
        operation.operation.id = fin_op.id_source_operation_sale       
)