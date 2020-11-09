cache totals for companies as main_company (
    select
        array_agg( child_company.id ) as child_companies_ids
    from companies as child_company
    where
        child_company.id_parent_company = main_company.id
)