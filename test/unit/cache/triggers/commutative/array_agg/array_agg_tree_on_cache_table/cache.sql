cache totals for companies as parent_company (
    select
        array_agg( child_company.id ) as children_companies_ids
    from companies as child_company
    where
        child_company.id_parent_company = parent_company.id
)