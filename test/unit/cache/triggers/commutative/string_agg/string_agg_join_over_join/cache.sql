cache totals for orders (
    select 

        string_agg(distinct unit_type.name, ', ') as units_types,
        string_agg(distinct unit_category.name, ', ') as units_categories

    from units
    
    left join unit_type on
        unit_type.id = units.id_unit_type
    
    left join unit_category on
        unit_category.id = unit_type.id_category
    
    where
        units.id_order = orders.id
)