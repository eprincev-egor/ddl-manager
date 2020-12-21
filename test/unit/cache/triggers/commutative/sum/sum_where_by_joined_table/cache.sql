cache totals for companies (
    select
        sum( orders.profit ) as orders_total

    from orders

    left join order_type on
        order_type.id = orders.id_order_type

    where
        orders.id_client = companies.id and
        orders.deleted = 0 and
        order_type.name in ('LCL', 'LTL')
)