cache totals for companies as child_company (
    select
        count(*) as parent_company_orders_count
    from orders
    where
        orders.id_client = child_company.id_parent_company
)