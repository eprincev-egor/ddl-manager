cache totals for companies (
    select sum( orders.profit ) as orders_total
    from orders
    where
        orders.companies_ids <@ array[ companies.id ] and
        orders.deleted = 0
)