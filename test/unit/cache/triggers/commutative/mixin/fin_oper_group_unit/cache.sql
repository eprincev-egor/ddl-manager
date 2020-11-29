cache fin_oper_group_unit for operation.unit  (
    select
        sum( group_unit.quantity ) as group_unit_quantity,
        sum( group_unit.netto_weight ) as group_unit_netto_weight,
        sum( group_unit.gross_weight ) as group_unit_gross_weight,
        sum( group_unit.volume ) as group_unit_volume,
        sum( group_unit.ldm ) as group_unit_ldm,
        sum( group_unit.quantity_pallet ) as group_unit_quantity_pallet,

        string_agg( distinct company_buyer.list_company_name, ', ' ) 
        filter (where company_buyer.list_company_name is not null) as group_unit_buyers_names,

        string_agg( distinct point_delivery.list_warehouse_name, ', ' ) 
        filter (where point_delivery.list_warehouse_name is not null) as group_unit_delivery_names

    from operation.fin_operation_group_unit as group_unit

    left join list_company as company_buyer on
        company_buyer.id = group_unit.id_company_buyer
    
    left join list_warehouse as point_delivery on
        point_delivery.id = group_unit.id_point_delivery

    where
        group_unit.deleted = 0 and
        group_unit.id_operation_unit = operation.unit.id
)
without triggers on list_company
without triggers on list_warehouse