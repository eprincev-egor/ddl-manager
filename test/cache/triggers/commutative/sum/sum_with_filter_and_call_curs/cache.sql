cache totals for orders (
    select

        sum(
            fin_operation.sum * get_curs(
                fin_operation.date,
                fin_operation.id_currency
            )
        ) filter (where
            fin_operation.id_fin_operation_type = 1
        ) as fin_operation_buys,

        sum(
            fin_operation.sum * get_curs(
                fin_operation.date,
                fin_operation.id_currency
            )
        ) filter (where
            fin_operation.id_fin_operation_type = 2
        ) as fin_operation_sales

    from fin_operation
    where
        fin_operation.id_order = orders.id and
        fin_operation.deleted = 0
)