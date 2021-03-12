create or replace function cache_totals_for_orders_on_units()
returns trigger as $body$
declare old_unit_type_name text;
declare old_unit_type_id_category integer;
declare old_category_name text;
declare new_unit_type_name text;
declare new_unit_type_id_category integer;
declare new_category_name text;
begin

    if TG_OP = 'DELETE' then

        if old.id_order is not null then
            if old.id_unit_type is not null then
                select
                    unit_type.name,
                    unit_type.id_category
                into
                    old_unit_type_name,
                    old_unit_type_id_category
                from unit_type
                where
                    unit_type.id = old.id_unit_type;
            end if;

            if old_unit_type_id_category is not null then
                old_category_name = (
                    select
                        unit_category.name
                    from unit_category
                    where
                        unit_category.id = old_unit_type_id_category
                );
            end if;

            update orders set
                units_types_name = cm_array_remove_one_element(
                    units_types_name,
                    old_unit_type_name
                ),
                units_types = (
                    select
                        string_agg(distinct item.name, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            units_types_name,
                            old_unit_type_name
                        )
                    ) as item(name)
                ),
                units_categories_name = cm_array_remove_one_element(
                    units_categories_name,
                    old_category_name
                ),
                units_categories = (
                    select
                        string_agg(distinct item.name, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            units_categories_name,
                            old_category_name
                        )
                    ) as item(name)
                )
            where
                old.id_order = orders.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_order is not distinct from old.id_order
            and
            new.id_unit_type is not distinct from old.id_unit_type
        then
            return new;
        end if;

        if old.id_unit_type is not null then
            select
                unit_type.name,
                unit_type.id_category
            into
                old_unit_type_name,
                old_unit_type_id_category
            from unit_type
            where
                unit_type.id = old.id_unit_type;
        end if;

        if old_unit_type_id_category is not null then
            old_category_name = (
                select
                    unit_category.name
                from unit_category
                where
                    unit_category.id = old_unit_type_id_category
            );
        end if;

        if new.id_unit_type is not distinct from old.id_unit_type then
            new_unit_type_name = old_unit_type_name;
            new_unit_type_id_category = old_unit_type_id_category;
        else
            if new.id_unit_type is not null then
                select
                    unit_type.name,
                    unit_type.id_category
                into
                    new_unit_type_name,
                    new_unit_type_id_category
                from unit_type
                where
                    unit_type.id = new.id_unit_type;
            end if;
        end if;

        if new_unit_type_id_category is not distinct from old_unit_type_id_category then
            new_category_name = old_category_name;
        else
            if new_unit_type_id_category is not null then
                new_category_name = (
                    select
                        unit_category.name
                    from unit_category
                    where
                        unit_category.id = new_unit_type_id_category
                );
            end if;
        end if;

        if new.id_order is not distinct from old.id_order then
            if new.id_order is null then
                return new;
            end if;

            update orders set
                units_types_name = array_append(
                    cm_array_remove_one_element(
                        units_types_name,
                        old_unit_type_name
                    ),
                    new_unit_type_name
                ),
                units_types = (
                    select
                        string_agg(distinct item.name, ', ')

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                units_types_name,
                                old_unit_type_name
                            ),
                            new_unit_type_name
                        )
                    ) as item(name)
                ),
                units_categories_name = array_append(
                    cm_array_remove_one_element(
                        units_categories_name,
                        old_category_name
                    ),
                    new_category_name
                ),
                units_categories = (
                    select
                        string_agg(distinct item.name, ', ')

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                units_categories_name,
                                old_category_name
                            ),
                            new_category_name
                        )
                    ) as item(name)
                )
            where
                new.id_order = orders.id;

            return new;
        end if;

        if old.id_order is not null then
            update orders set
                units_types_name = cm_array_remove_one_element(
                    units_types_name,
                    old_unit_type_name
                ),
                units_types = (
                    select
                        string_agg(distinct item.name, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            units_types_name,
                            old_unit_type_name
                        )
                    ) as item(name)
                ),
                units_categories_name = cm_array_remove_one_element(
                    units_categories_name,
                    old_category_name
                ),
                units_categories = (
                    select
                        string_agg(distinct item.name, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            units_categories_name,
                            old_category_name
                        )
                    ) as item(name)
                )
            where
                old.id_order = orders.id;
        end if;

        if new.id_order is not null then
            update orders set
                units_types_name = array_append(
                    units_types_name,
                    new_unit_type_name
                ),
                units_types = case
                    when
                        array_position(
                            units_types_name,
                            new_unit_type_name
                        )
                        is null
                    then
                        coalesce(
                            units_types ||
                            coalesce(
                                ', '
                                || new_unit_type_name,
                                ''
                            ),
                            new_unit_type_name
                        )
                    else
                        units_types
                end,
                units_categories_name = array_append(
                    units_categories_name,
                    new_category_name
                ),
                units_categories = case
                    when
                        array_position(
                            units_categories_name,
                            new_category_name
                        )
                        is null
                    then
                        coalesce(
                            units_categories ||
                            coalesce(
                                ', '
                                || new_category_name,
                                ''
                            ),
                            new_category_name
                        )
                    else
                        units_categories
                end
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_order is not null then
            if new.id_unit_type is not null then
                select
                    unit_type.name,
                    unit_type.id_category
                into
                    new_unit_type_name,
                    new_unit_type_id_category
                from unit_type
                where
                    unit_type.id = new.id_unit_type;
            end if;

            if new_unit_type_id_category is not null then
                new_category_name = (
                    select
                        unit_category.name
                    from unit_category
                    where
                        unit_category.id = new_unit_type_id_category
                );
            end if;

            update orders set
                units_types_name = array_append(
                    units_types_name,
                    new_unit_type_name
                ),
                units_types = case
                    when
                        array_position(
                            units_types_name,
                            new_unit_type_name
                        )
                        is null
                    then
                        coalesce(
                            units_types ||
                            coalesce(
                                ', '
                                || new_unit_type_name,
                                ''
                            ),
                            new_unit_type_name
                        )
                    else
                        units_types
                end,
                units_categories_name = array_append(
                    units_categories_name,
                    new_category_name
                ),
                units_categories = case
                    when
                        array_position(
                            units_categories_name,
                            new_category_name
                        )
                        is null
                    then
                        coalesce(
                            units_categories ||
                            coalesce(
                                ', '
                                || new_category_name,
                                ''
                            ),
                            new_category_name
                        )
                    else
                        units_categories
                end
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_units
after insert or update of id_order, id_unit_type or delete
on public.units
for each row
execute procedure cache_totals_for_orders_on_units();