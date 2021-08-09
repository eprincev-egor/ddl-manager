create or replace function cache_fin_oper_group_unit_for_unit_on_fin_operation_group_unit()
returns trigger as $body$
declare old_company_buyer_list_company_name text;
declare old_point_delivery_list_warehouse_name text;
declare new_company_buyer_list_company_name text;
declare new_point_delivery_list_warehouse_name text;
begin

    if TG_OP = 'DELETE' then

        if
            old.id_operation_unit is not null
            and
            old.deleted = 0
        then
            if old.id_company_buyer is not null then
                old_company_buyer_list_company_name = (
                    select
                        list_company.list_company_name
                    from list_company
                    where
                        list_company.id = old.id_company_buyer
                );
            end if;

            if old.id_point_delivery is not null then
                old_point_delivery_list_warehouse_name = (
                    select
                        list_warehouse.list_warehouse_name
                    from list_warehouse
                    where
                        list_warehouse.id = old.id_point_delivery
                );
            end if;

            update operation.unit set
                group_unit_quantity = coalesce(group_unit_quantity, 0) - coalesce(old.quantity, 0),
                group_unit_netto_weight = coalesce(
                    group_unit_netto_weight,
                    0
                ) - coalesce(old.netto_weight, 0),
                group_unit_gross_weight = coalesce(
                    group_unit_gross_weight,
                    0
                ) - coalesce(old.gross_weight, 0),
                group_unit_volume = coalesce(group_unit_volume, 0) - coalesce(old.volume, 0),
                group_unit_ldm = coalesce(group_unit_ldm, 0) - coalesce(old.ldm, 0),
                group_unit_quantity_pallet = coalesce(
                    group_unit_quantity_pallet,
                    0
                ) - coalesce(old.quantity_pallet, 0),
                group_unit_buyers_names_list_company_name = case
                    when
                        old_company_buyer_list_company_name is not null
                    then
                        cm_array_remove_one_element(
                            group_unit_buyers_names_list_company_name,
                            old_company_buyer_list_company_name
                        )
                    else
                        group_unit_buyers_names_list_company_name
                end,
                group_unit_buyers_names = case
                    when
                        old_company_buyer_list_company_name is not null
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.list_company_name,
                                    ', '
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    group_unit_buyers_names_list_company_name,
                                    old_company_buyer_list_company_name
                                )
                            ) as item(list_company_name)
                        )
                    else
                        group_unit_buyers_names
                end,
                group_unit_delivery_names_list_warehouse_name = case
                    when
                        old_point_delivery_list_warehouse_name is not null
                    then
                        cm_array_remove_one_element(
                            group_unit_delivery_names_list_warehouse_name,
                            old_point_delivery_list_warehouse_name
                        )
                    else
                        group_unit_delivery_names_list_warehouse_name
                end,
                group_unit_delivery_names = case
                    when
                        old_point_delivery_list_warehouse_name is not null
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.list_warehouse_name,
                                    ', '
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    group_unit_delivery_names_list_warehouse_name,
                                    old_point_delivery_list_warehouse_name
                                )
                            ) as item(list_warehouse_name)
                        )
                    else
                        group_unit_delivery_names
                end
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

        if old.id_company_buyer is not null then
            old_company_buyer_list_company_name = (
                select
                    list_company.list_company_name
                from list_company
                where
                    list_company.id = old.id_company_buyer
            );
        end if;

        if old.id_point_delivery is not null then
            old_point_delivery_list_warehouse_name = (
                select
                    list_warehouse.list_warehouse_name
                from list_warehouse
                where
                    list_warehouse.id = old.id_point_delivery
            );
        end if;

        if new.id_company_buyer is not distinct from old.id_company_buyer then
            new_company_buyer_list_company_name = old_company_buyer_list_company_name;
        else
            if new.id_company_buyer is not null then
                new_company_buyer_list_company_name = (
                    select
                        list_company.list_company_name
                    from list_company
                    where
                        list_company.id = new.id_company_buyer
                );
            end if;
        end if;

        if new.id_point_delivery is not distinct from old.id_point_delivery then
            new_point_delivery_list_warehouse_name = old_point_delivery_list_warehouse_name;
        else
            if new.id_point_delivery is not null then
                new_point_delivery_list_warehouse_name = (
                    select
                        list_warehouse.list_warehouse_name
                    from list_warehouse
                    where
                        list_warehouse.id = new.id_point_delivery
                );
            end if;
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
                group_unit_quantity = coalesce(group_unit_quantity, 0) - coalesce(old.quantity, 0) + coalesce(new.quantity, 0),
                group_unit_netto_weight = coalesce(
                    group_unit_netto_weight,
                    0
                ) - coalesce(old.netto_weight, 0) + coalesce(new.netto_weight, 0),
                group_unit_gross_weight = coalesce(
                    group_unit_gross_weight,
                    0
                ) - coalesce(old.gross_weight, 0) + coalesce(new.gross_weight, 0),
                group_unit_volume = coalesce(group_unit_volume, 0) - coalesce(old.volume, 0) + coalesce(new.volume, 0),
                group_unit_ldm = coalesce(group_unit_ldm, 0) - coalesce(old.ldm, 0) + coalesce(new.ldm, 0),
                group_unit_quantity_pallet = coalesce(
                    group_unit_quantity_pallet,
                    0
                ) - coalesce(old.quantity_pallet, 0) + coalesce(new.quantity_pallet, 0),
                group_unit_buyers_names_list_company_name = case
                    when
                        new_company_buyer_list_company_name is not null
                        and
                        not coalesce(old_company_buyer_list_company_name is not null, false)
                    then
                        array_append(
                            group_unit_buyers_names_list_company_name,
                            new_company_buyer_list_company_name
                        )
                    when
                        not coalesce(new_company_buyer_list_company_name is not null, false)
                        and
                        old_company_buyer_list_company_name is not null
                    then
                        cm_array_remove_one_element(
                            group_unit_buyers_names_list_company_name,
                            old_company_buyer_list_company_name
                        )
                    when
                        new_company_buyer_list_company_name is not null
                    then
                        array_append(
                            cm_array_remove_one_element(
                                group_unit_buyers_names_list_company_name,
                                old_company_buyer_list_company_name
                            ),
                            new_company_buyer_list_company_name
                        )
                    else
                        group_unit_buyers_names_list_company_name
                end,
                group_unit_buyers_names = case
                    when
                        new_company_buyer_list_company_name is not null
                        and
                        not coalesce(old_company_buyer_list_company_name is not null, false)
                    then
                        case
                            when
                                array_position(
                                    group_unit_buyers_names_list_company_name,
                                    new_company_buyer_list_company_name
                                )
                                is null
                            then
                                coalesce(
                                    group_unit_buyers_names ||
                                    coalesce(
                                        ', '
                                        || new_company_buyer_list_company_name,
                                        ''
                                    ),
                                    new_company_buyer_list_company_name
                                )
                            else
                                group_unit_buyers_names
                        end
                    when
                        not coalesce(new_company_buyer_list_company_name is not null, false)
                        and
                        old_company_buyer_list_company_name is not null
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.list_company_name,
                                    ', '
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    group_unit_buyers_names_list_company_name,
                                    old_company_buyer_list_company_name
                                )
                            ) as item(list_company_name)
                        )
                    when
                        new_company_buyer_list_company_name is not null
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.list_company_name,
                                    ', '
                                )

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        group_unit_buyers_names_list_company_name,
                                        old_company_buyer_list_company_name
                                    ),
                                    new_company_buyer_list_company_name
                                )
                            ) as item(list_company_name)
                        )
                    else
                        group_unit_buyers_names
                end,
                group_unit_delivery_names_list_warehouse_name = case
                    when
                        new_point_delivery_list_warehouse_name is not null
                        and
                        not coalesce(old_point_delivery_list_warehouse_name is not null, false)
                    then
                        array_append(
                            group_unit_delivery_names_list_warehouse_name,
                            new_point_delivery_list_warehouse_name
                        )
                    when
                        not coalesce(new_point_delivery_list_warehouse_name is not null, false)
                        and
                        old_point_delivery_list_warehouse_name is not null
                    then
                        cm_array_remove_one_element(
                            group_unit_delivery_names_list_warehouse_name,
                            old_point_delivery_list_warehouse_name
                        )
                    when
                        new_point_delivery_list_warehouse_name is not null
                    then
                        array_append(
                            cm_array_remove_one_element(
                                group_unit_delivery_names_list_warehouse_name,
                                old_point_delivery_list_warehouse_name
                            ),
                            new_point_delivery_list_warehouse_name
                        )
                    else
                        group_unit_delivery_names_list_warehouse_name
                end,
                group_unit_delivery_names = case
                    when
                        new_point_delivery_list_warehouse_name is not null
                        and
                        not coalesce(old_point_delivery_list_warehouse_name is not null, false)
                    then
                        case
                            when
                                array_position(
                                    group_unit_delivery_names_list_warehouse_name,
                                    new_point_delivery_list_warehouse_name
                                )
                                is null
                            then
                                coalesce(
                                    group_unit_delivery_names ||
                                    coalesce(
                                        ', '
                                        || new_point_delivery_list_warehouse_name,
                                        ''
                                    ),
                                    new_point_delivery_list_warehouse_name
                                )
                            else
                                group_unit_delivery_names
                        end
                    when
                        not coalesce(new_point_delivery_list_warehouse_name is not null, false)
                        and
                        old_point_delivery_list_warehouse_name is not null
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.list_warehouse_name,
                                    ', '
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    group_unit_delivery_names_list_warehouse_name,
                                    old_point_delivery_list_warehouse_name
                                )
                            ) as item(list_warehouse_name)
                        )
                    when
                        new_point_delivery_list_warehouse_name is not null
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.list_warehouse_name,
                                    ', '
                                )

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        group_unit_delivery_names_list_warehouse_name,
                                        old_point_delivery_list_warehouse_name
                                    ),
                                    new_point_delivery_list_warehouse_name
                                )
                            ) as item(list_warehouse_name)
                        )
                    else
                        group_unit_delivery_names
                end
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
                group_unit_quantity = coalesce(group_unit_quantity, 0) - coalesce(old.quantity, 0),
                group_unit_netto_weight = coalesce(
                    group_unit_netto_weight,
                    0
                ) - coalesce(old.netto_weight, 0),
                group_unit_gross_weight = coalesce(
                    group_unit_gross_weight,
                    0
                ) - coalesce(old.gross_weight, 0),
                group_unit_volume = coalesce(group_unit_volume, 0) - coalesce(old.volume, 0),
                group_unit_ldm = coalesce(group_unit_ldm, 0) - coalesce(old.ldm, 0),
                group_unit_quantity_pallet = coalesce(
                    group_unit_quantity_pallet,
                    0
                ) - coalesce(old.quantity_pallet, 0),
                group_unit_buyers_names_list_company_name = case
                    when
                        old_company_buyer_list_company_name is not null
                    then
                        cm_array_remove_one_element(
                            group_unit_buyers_names_list_company_name,
                            old_company_buyer_list_company_name
                        )
                    else
                        group_unit_buyers_names_list_company_name
                end,
                group_unit_buyers_names = case
                    when
                        old_company_buyer_list_company_name is not null
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.list_company_name,
                                    ', '
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    group_unit_buyers_names_list_company_name,
                                    old_company_buyer_list_company_name
                                )
                            ) as item(list_company_name)
                        )
                    else
                        group_unit_buyers_names
                end,
                group_unit_delivery_names_list_warehouse_name = case
                    when
                        old_point_delivery_list_warehouse_name is not null
                    then
                        cm_array_remove_one_element(
                            group_unit_delivery_names_list_warehouse_name,
                            old_point_delivery_list_warehouse_name
                        )
                    else
                        group_unit_delivery_names_list_warehouse_name
                end,
                group_unit_delivery_names = case
                    when
                        old_point_delivery_list_warehouse_name is not null
                    then
                        (
                            select
                                string_agg(distinct 
                                    item.list_warehouse_name,
                                    ', '
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    group_unit_delivery_names_list_warehouse_name,
                                    old_point_delivery_list_warehouse_name
                                )
                            ) as item(list_warehouse_name)
                        )
                    else
                        group_unit_delivery_names
                end
            where
                old.id_operation_unit = operation.unit.id;
        end if;

        if
            new.id_operation_unit is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                group_unit_quantity = coalesce(group_unit_quantity, 0) + coalesce(new.quantity, 0),
                group_unit_netto_weight = coalesce(
                    group_unit_netto_weight,
                    0
                ) + coalesce(new.netto_weight, 0),
                group_unit_gross_weight = coalesce(
                    group_unit_gross_weight,
                    0
                ) + coalesce(new.gross_weight, 0),
                group_unit_volume = coalesce(group_unit_volume, 0) + coalesce(new.volume, 0),
                group_unit_ldm = coalesce(group_unit_ldm, 0) + coalesce(new.ldm, 0),
                group_unit_quantity_pallet = coalesce(
                    group_unit_quantity_pallet,
                    0
                ) + coalesce(new.quantity_pallet, 0),
                group_unit_buyers_names_list_company_name = case
                    when
                        new_company_buyer_list_company_name is not null
                    then
                        array_append(
                            group_unit_buyers_names_list_company_name,
                            new_company_buyer_list_company_name
                        )
                    else
                        group_unit_buyers_names_list_company_name
                end,
                group_unit_buyers_names = case
                    when
                        new_company_buyer_list_company_name is not null
                    then
                        case
                            when
                                array_position(
                                    group_unit_buyers_names_list_company_name,
                                    new_company_buyer_list_company_name
                                )
                                is null
                            then
                                coalesce(
                                    group_unit_buyers_names ||
                                    coalesce(
                                        ', '
                                        || new_company_buyer_list_company_name,
                                        ''
                                    ),
                                    new_company_buyer_list_company_name
                                )
                            else
                                group_unit_buyers_names
                        end
                    else
                        group_unit_buyers_names
                end,
                group_unit_delivery_names_list_warehouse_name = case
                    when
                        new_point_delivery_list_warehouse_name is not null
                    then
                        array_append(
                            group_unit_delivery_names_list_warehouse_name,
                            new_point_delivery_list_warehouse_name
                        )
                    else
                        group_unit_delivery_names_list_warehouse_name
                end,
                group_unit_delivery_names = case
                    when
                        new_point_delivery_list_warehouse_name is not null
                    then
                        case
                            when
                                array_position(
                                    group_unit_delivery_names_list_warehouse_name,
                                    new_point_delivery_list_warehouse_name
                                )
                                is null
                            then
                                coalesce(
                                    group_unit_delivery_names ||
                                    coalesce(
                                        ', '
                                        || new_point_delivery_list_warehouse_name,
                                        ''
                                    ),
                                    new_point_delivery_list_warehouse_name
                                )
                            else
                                group_unit_delivery_names
                        end
                    else
                        group_unit_delivery_names
                end
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
            if new.id_company_buyer is not null then
                new_company_buyer_list_company_name = (
                    select
                        list_company.list_company_name
                    from list_company
                    where
                        list_company.id = new.id_company_buyer
                );
            end if;

            if new.id_point_delivery is not null then
                new_point_delivery_list_warehouse_name = (
                    select
                        list_warehouse.list_warehouse_name
                    from list_warehouse
                    where
                        list_warehouse.id = new.id_point_delivery
                );
            end if;

            update operation.unit set
                group_unit_quantity = coalesce(group_unit_quantity, 0) + coalesce(new.quantity, 0),
                group_unit_netto_weight = coalesce(
                    group_unit_netto_weight,
                    0
                ) + coalesce(new.netto_weight, 0),
                group_unit_gross_weight = coalesce(
                    group_unit_gross_weight,
                    0
                ) + coalesce(new.gross_weight, 0),
                group_unit_volume = coalesce(group_unit_volume, 0) + coalesce(new.volume, 0),
                group_unit_ldm = coalesce(group_unit_ldm, 0) + coalesce(new.ldm, 0),
                group_unit_quantity_pallet = coalesce(
                    group_unit_quantity_pallet,
                    0
                ) + coalesce(new.quantity_pallet, 0),
                group_unit_buyers_names_list_company_name = case
                    when
                        new_company_buyer_list_company_name is not null
                    then
                        array_append(
                            group_unit_buyers_names_list_company_name,
                            new_company_buyer_list_company_name
                        )
                    else
                        group_unit_buyers_names_list_company_name
                end,
                group_unit_buyers_names = case
                    when
                        new_company_buyer_list_company_name is not null
                    then
                        case
                            when
                                array_position(
                                    group_unit_buyers_names_list_company_name,
                                    new_company_buyer_list_company_name
                                )
                                is null
                            then
                                coalesce(
                                    group_unit_buyers_names ||
                                    coalesce(
                                        ', '
                                        || new_company_buyer_list_company_name,
                                        ''
                                    ),
                                    new_company_buyer_list_company_name
                                )
                            else
                                group_unit_buyers_names
                        end
                    else
                        group_unit_buyers_names
                end,
                group_unit_delivery_names_list_warehouse_name = case
                    when
                        new_point_delivery_list_warehouse_name is not null
                    then
                        array_append(
                            group_unit_delivery_names_list_warehouse_name,
                            new_point_delivery_list_warehouse_name
                        )
                    else
                        group_unit_delivery_names_list_warehouse_name
                end,
                group_unit_delivery_names = case
                    when
                        new_point_delivery_list_warehouse_name is not null
                    then
                        case
                            when
                                array_position(
                                    group_unit_delivery_names_list_warehouse_name,
                                    new_point_delivery_list_warehouse_name
                                )
                                is null
                            then
                                coalesce(
                                    group_unit_delivery_names ||
                                    coalesce(
                                        ', '
                                        || new_point_delivery_list_warehouse_name,
                                        ''
                                    ),
                                    new_point_delivery_list_warehouse_name
                                )
                            else
                                group_unit_delivery_names
                        end
                    else
                        group_unit_delivery_names
                end
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