cache cargo_totals for orders (
    select 
        sum( cargos.total_weight ) as cargos_weight,
        string_agg( product_types.name, ', ' ) as cargos_products_names
    from cargos

    left join product_types on
        product_types.id = cargos.id_product_type

    where
        cargos.id_order = orders.id
)