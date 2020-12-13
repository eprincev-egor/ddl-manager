create or replace function cache_cargo_totals_for_orders_on_cargos()
returns trigger as $body$
declare old_product_type_name text;
declare new_product_type_name text;
begin

    if TG_OP = 'DELETE' then

        if old.id_order is not null then

            if old.id_product_type is not null then
                old_product_type_name = (
                    select
                        product_types.name
                    from product_types
                    where
                        product_types.id = old.id_product_type
                );
            end if;

            if
                coalesce(old.total_weight, 0) != 0
                or
                old_product_type_name is not null
            then
                update orders set
                    cargos_weight = cargos_weight - coalesce(old.total_weight, 0),
                    cargos_products_names_name = cm_array_remove_one_element(
                        cargos_products_names_name,
                        old_product_type_name
                    ),
                    cargos_products_names = (
                        select
                            string_agg(item.name, ', ')

                        from unnest(
                            cm_array_remove_one_element(
                                cargos_products_names_name,
                                old_product_type_name
                            )
                        ) as item(name)
                    )
                where
                    old.id_order = orders.id;
            end if;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_order is not distinct from old.id_order
            and
            new.id_product_type is not distinct from old.id_product_type
            and
            new.total_weight is not distinct from old.total_weight
        then
            return new;
        end if;

        if old.id_product_type is not null then
            old_product_type_name = (
                select
                    product_types.name
                from product_types
                where
                    product_types.id = old.id_product_type
            );
        end if;

        if new.id_product_type is not distinct from old.id_product_type then
            new_product_type_name = old_product_type_name;
        else
            if new.id_product_type is not null then
                new_product_type_name = (
                    select
                        product_types.name
                    from product_types
                    where
                        product_types.id = new.id_product_type
                );
            end if;
        end if;


        if new.id_order is not distinct from old.id_order then

            update orders set
                cargos_weight = cargos_weight - coalesce(old.total_weight, 0) + coalesce(new.total_weight, 0),
                cargos_products_names_name = array_append(
                    cm_array_remove_one_element(
                        cargos_products_names_name,
                        old_product_type_name
                    ),
                    new_product_type_name
                ),
                cargos_products_names = (
                    select
                        string_agg(item.name, ', ')

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                cargos_products_names_name,
                                old_product_type_name
                            ),
                            new_product_type_name
                        )
                    ) as item(name)
                )
            where
                new.id_order = orders.id;

            return new;
        end if;


        if
            old.id_order is not null
            and
            (
                coalesce(old.total_weight, 0) != 0
                or
                old_product_type_name is not null
            )
        then
            update orders set
                cargos_weight = cargos_weight - coalesce(old.total_weight, 0),
                cargos_products_names_name = cm_array_remove_one_element(
                    cargos_products_names_name,
                    old_product_type_name
                ),
                cargos_products_names = (
                    select
                        string_agg(item.name, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            cargos_products_names_name,
                            old_product_type_name
                        )
                    ) as item(name)
                )
            where
                old.id_order = orders.id;
        end if;

        if
            new.id_order is not null
            and
            (
                coalesce(new.total_weight, 0) != 0
                or
                new_product_type_name is not null
            )
        then
            update orders set
                cargos_weight = cargos_weight + coalesce(new.total_weight, 0),
                cargos_products_names_name = array_append(
                    cargos_products_names_name,
                    new_product_type_name
                ),
                cargos_products_names = (
                    select
                        string_agg(item.name, ', ')

                    from unnest(
                        array_append(
                            cargos_products_names_name,
                            new_product_type_name
                        )
                    ) as item(name)
                )
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_order is not null then

            if new.id_product_type is not null then
                new_product_type_name = (
                    select
                        product_types.name
                    from product_types
                    where
                        product_types.id = new.id_product_type
                );
            end if;

            if
                coalesce(new.total_weight, 0) != 0
                or
                new_product_type_name is not null
            then
                update orders set
                    cargos_weight = cargos_weight + coalesce(new.total_weight, 0),
                    cargos_products_names_name = array_append(
                        cargos_products_names_name,
                        new_product_type_name
                    ),
                    cargos_products_names = (
                        select
                            string_agg(item.name, ', ')

                        from unnest(
                            array_append(
                                cargos_products_names_name,
                                new_product_type_name
                            )
                        ) as item(name)
                    )
                where
                    new.id_order = orders.id;
            end if;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_cargo_totals_for_orders_on_cargos
after insert or update of id_order, id_product_type, total_weight or delete
on public.cargos
for each row
execute procedure cache_cargo_totals_for_orders_on_cargos();