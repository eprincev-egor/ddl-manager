create or replace function cache_fin_oper_group_unit_for_unit_on_fin_operation_group_unit()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_operation_unit is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                __fin_oper_group_unit_json__ = __fin_oper_group_unit_json__ - old.id::text,
                (
                    group_unit_quantity,
                    group_unit_netto_weight,
                    group_unit_gross_weight,
                    group_unit_volume,
                    group_unit_ldm,
                    group_unit_quantity_pallet,
                    group_unit_buyers_names,
                    group_unit_delivery_names
                ) = (
                    select
                            sum(source_row.quantity) as group_unit_quantity,
                            sum(source_row.netto_weight) as group_unit_netto_weight,
                            sum(source_row.gross_weight) as group_unit_gross_weight,
                            sum(source_row.volume) as group_unit_volume,
                            sum(source_row.ldm) as group_unit_ldm,
                            sum(
                                source_row.quantity_pallet
                                                        ) as group_unit_quantity_pallet,
                            string_agg(distinct 
                                company_buyer.list_company_name,
                                ', '
                                                        ) filter (where     company_buyer.list_company_name is not null) as group_unit_buyers_names,
                            string_agg(distinct 
                                point_delivery.list_warehouse_name,
                                ', '
                                                        ) filter (where     point_delivery.list_warehouse_name is not null) as group_unit_delivery_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    __fin_oper_group_unit_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.fin_operation_group_unit, json_entry.value) as record on
                            true
                    ) as source_row

                    left join list_company as company_buyer on
                        company_buyer.id = source_row.id_company_buyer

                    left join list_warehouse as point_delivery on
                        point_delivery.id = source_row.id_point_delivery
                    where
                        source_row.deleted = 0
                        and
                        source_row.id_operation_unit = operation.unit.id
                )
            where
                old.id_operation_unit = operation.unit.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.gross_weight is not distinct from old.gross_weight
            and
            new.id_company_buyer is not distinct from old.id_company_buyer
            and
            new.id_operation_unit is not distinct from old.id_operation_unit
            and
            new.id_point_delivery is not distinct from old.id_point_delivery
            and
            new.ldm is not distinct from old.ldm
            and
            new.netto_weight is not distinct from old.netto_weight
            and
            new.quantity is not distinct from old.quantity
            and
            new.quantity_pallet is not distinct from old.quantity_pallet
            and
            new.volume is not distinct from old.volume
        then
            return new;
        end if;

        if
            new.id_operation_unit is not distinct from old.id_operation_unit
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_operation_unit is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update operation.unit set
                __fin_oper_group_unit_json__ = cm_merge_json(
            __fin_oper_group_unit_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'gross_weight', new.gross_weight,'id', new.id,'id_company_buyer', new.id_company_buyer,'id_operation_unit', new.id_operation_unit,'id_point_delivery', new.id_point_delivery,'ldm', new.ldm,'netto_weight', new.netto_weight,'quantity', new.quantity,'quantity_pallet', new.quantity_pallet,'volume', new.volume
        ),
            TG_OP
        ),
                (
                    group_unit_quantity,
                    group_unit_netto_weight,
                    group_unit_gross_weight,
                    group_unit_volume,
                    group_unit_ldm,
                    group_unit_quantity_pallet,
                    group_unit_buyers_names,
                    group_unit_delivery_names
                ) = (
                    select
                            sum(source_row.quantity) as group_unit_quantity,
                            sum(source_row.netto_weight) as group_unit_netto_weight,
                            sum(source_row.gross_weight) as group_unit_gross_weight,
                            sum(source_row.volume) as group_unit_volume,
                            sum(source_row.ldm) as group_unit_ldm,
                            sum(
                                source_row.quantity_pallet
                                                        ) as group_unit_quantity_pallet,
                            string_agg(distinct 
                                company_buyer.list_company_name,
                                ', '
                                                        ) filter (where     company_buyer.list_company_name is not null) as group_unit_buyers_names,
                            string_agg(distinct 
                                point_delivery.list_warehouse_name,
                                ', '
                                                        ) filter (where     point_delivery.list_warehouse_name is not null) as group_unit_delivery_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __fin_oper_group_unit_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'gross_weight', new.gross_weight,'id', new.id,'id_company_buyer', new.id_company_buyer,'id_operation_unit', new.id_operation_unit,'id_point_delivery', new.id_point_delivery,'ldm', new.ldm,'netto_weight', new.netto_weight,'quantity', new.quantity,'quantity_pallet', new.quantity_pallet,'volume', new.volume
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.fin_operation_group_unit, json_entry.value) as record on
                            true
                    ) as source_row

                    left join list_company as company_buyer on
                        company_buyer.id = source_row.id_company_buyer

                    left join list_warehouse as point_delivery on
                        point_delivery.id = source_row.id_point_delivery
                    where
                        source_row.deleted = 0
                        and
                        source_row.id_operation_unit = operation.unit.id
                )
            where
                new.id_operation_unit = operation.unit.id;

            return new;
        end if;

        if
            old.id_operation_unit is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                __fin_oper_group_unit_json__ = __fin_oper_group_unit_json__ - old.id::text,
                (
                    group_unit_quantity,
                    group_unit_netto_weight,
                    group_unit_gross_weight,
                    group_unit_volume,
                    group_unit_ldm,
                    group_unit_quantity_pallet,
                    group_unit_buyers_names,
                    group_unit_delivery_names
                ) = (
                    select
                            sum(source_row.quantity) as group_unit_quantity,
                            sum(source_row.netto_weight) as group_unit_netto_weight,
                            sum(source_row.gross_weight) as group_unit_gross_weight,
                            sum(source_row.volume) as group_unit_volume,
                            sum(source_row.ldm) as group_unit_ldm,
                            sum(
                                source_row.quantity_pallet
                                                        ) as group_unit_quantity_pallet,
                            string_agg(distinct 
                                company_buyer.list_company_name,
                                ', '
                                                        ) filter (where     company_buyer.list_company_name is not null) as group_unit_buyers_names,
                            string_agg(distinct 
                                point_delivery.list_warehouse_name,
                                ', '
                                                        ) filter (where     point_delivery.list_warehouse_name is not null) as group_unit_delivery_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    __fin_oper_group_unit_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.fin_operation_group_unit, json_entry.value) as record on
                            true
                    ) as source_row

                    left join list_company as company_buyer on
                        company_buyer.id = source_row.id_company_buyer

                    left join list_warehouse as point_delivery on
                        point_delivery.id = source_row.id_point_delivery
                    where
                        source_row.deleted = 0
                        and
                        source_row.id_operation_unit = operation.unit.id
                )
            where
                old.id_operation_unit = operation.unit.id;
        end if;

        if
            new.id_operation_unit is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                __fin_oper_group_unit_json__ = cm_merge_json(
            __fin_oper_group_unit_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'gross_weight', new.gross_weight,'id', new.id,'id_company_buyer', new.id_company_buyer,'id_operation_unit', new.id_operation_unit,'id_point_delivery', new.id_point_delivery,'ldm', new.ldm,'netto_weight', new.netto_weight,'quantity', new.quantity,'quantity_pallet', new.quantity_pallet,'volume', new.volume
        ),
            TG_OP
        ),
                (
                    group_unit_quantity,
                    group_unit_netto_weight,
                    group_unit_gross_weight,
                    group_unit_volume,
                    group_unit_ldm,
                    group_unit_quantity_pallet,
                    group_unit_buyers_names,
                    group_unit_delivery_names
                ) = (
                    select
                            sum(source_row.quantity) as group_unit_quantity,
                            sum(source_row.netto_weight) as group_unit_netto_weight,
                            sum(source_row.gross_weight) as group_unit_gross_weight,
                            sum(source_row.volume) as group_unit_volume,
                            sum(source_row.ldm) as group_unit_ldm,
                            sum(
                                source_row.quantity_pallet
                                                        ) as group_unit_quantity_pallet,
                            string_agg(distinct 
                                company_buyer.list_company_name,
                                ', '
                                                        ) filter (where     company_buyer.list_company_name is not null) as group_unit_buyers_names,
                            string_agg(distinct 
                                point_delivery.list_warehouse_name,
                                ', '
                                                        ) filter (where     point_delivery.list_warehouse_name is not null) as group_unit_delivery_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __fin_oper_group_unit_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'gross_weight', new.gross_weight,'id', new.id,'id_company_buyer', new.id_company_buyer,'id_operation_unit', new.id_operation_unit,'id_point_delivery', new.id_point_delivery,'ldm', new.ldm,'netto_weight', new.netto_weight,'quantity', new.quantity,'quantity_pallet', new.quantity_pallet,'volume', new.volume
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.fin_operation_group_unit, json_entry.value) as record on
                            true
                    ) as source_row

                    left join list_company as company_buyer on
                        company_buyer.id = source_row.id_company_buyer

                    left join list_warehouse as point_delivery on
                        point_delivery.id = source_row.id_point_delivery
                    where
                        source_row.deleted = 0
                        and
                        source_row.id_operation_unit = operation.unit.id
                )
            where
                new.id_operation_unit = operation.unit.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_operation_unit is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                __fin_oper_group_unit_json__ = cm_merge_json(
            __fin_oper_group_unit_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'gross_weight', new.gross_weight,'id', new.id,'id_company_buyer', new.id_company_buyer,'id_operation_unit', new.id_operation_unit,'id_point_delivery', new.id_point_delivery,'ldm', new.ldm,'netto_weight', new.netto_weight,'quantity', new.quantity,'quantity_pallet', new.quantity_pallet,'volume', new.volume
        ),
            TG_OP
        ),
                (
                    group_unit_quantity,
                    group_unit_netto_weight,
                    group_unit_gross_weight,
                    group_unit_volume,
                    group_unit_ldm,
                    group_unit_quantity_pallet,
                    group_unit_buyers_names,
                    group_unit_delivery_names
                ) = (
                    select
                            sum(source_row.quantity) as group_unit_quantity,
                            sum(source_row.netto_weight) as group_unit_netto_weight,
                            sum(source_row.gross_weight) as group_unit_gross_weight,
                            sum(source_row.volume) as group_unit_volume,
                            sum(source_row.ldm) as group_unit_ldm,
                            sum(
                                source_row.quantity_pallet
                                                        ) as group_unit_quantity_pallet,
                            string_agg(distinct 
                                company_buyer.list_company_name,
                                ', '
                                                        ) filter (where     company_buyer.list_company_name is not null) as group_unit_buyers_names,
                            string_agg(distinct 
                                point_delivery.list_warehouse_name,
                                ', '
                                                        ) filter (where     point_delivery.list_warehouse_name is not null) as group_unit_delivery_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __fin_oper_group_unit_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'gross_weight', new.gross_weight,'id', new.id,'id_company_buyer', new.id_company_buyer,'id_operation_unit', new.id_operation_unit,'id_point_delivery', new.id_point_delivery,'ldm', new.ldm,'netto_weight', new.netto_weight,'quantity', new.quantity,'quantity_pallet', new.quantity_pallet,'volume', new.volume
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::operation.fin_operation_group_unit, json_entry.value) as record on
                            true
                    ) as source_row

                    left join list_company as company_buyer on
                        company_buyer.id = source_row.id_company_buyer

                    left join list_warehouse as point_delivery on
                        point_delivery.id = source_row.id_point_delivery
                    where
                        source_row.deleted = 0
                        and
                        source_row.id_operation_unit = operation.unit.id
                )
            where
                new.id_operation_unit = operation.unit.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_fin_oper_group_unit_for_unit_on_fin_operation_group_unit
after insert or update of deleted, gross_weight, id_company_buyer, id_operation_unit, id_point_delivery, ldm, netto_weight, quantity, quantity_pallet, volume or delete
on operation.fin_operation_group_unit
for each row
execute procedure cache_fin_oper_group_unit_for_unit_on_fin_operation_group_unit();