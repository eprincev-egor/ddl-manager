cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        array[ companies.id ] <@ orders.companies_ids and
        orders.deleted = 0
)