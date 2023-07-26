create or replace function cache_cargo_totals_for_orders_on_cargos()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_order is not null then
            update orders set
                __cargo_totals_json__ = __cargo_totals_json__ - old.id::text,
                (
                    cargos_weight,
                    cargos_products_names
                ) = (
                    select
                            sum(source_row.total_weight) as cargos_weight,
                            string_agg(product_types.name, ', ') as cargos_products_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    __cargo_totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.cargos, json_entry.value) as record on
                            true
                    ) as source_row

                    left join product_types on
                        product_types.id = source_row.id_product_type
                    where
                        source_row.id_order = orders.id
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
            new.id_product_type is not distinct from old.id_product_type
            and
            new.total_weight is not distinct from old.total_weight
        then
            return new;
        end if;

        if new.id_order is not distinct from old.id_order then
            if new.id_order is null then
                return new;
            end if;

            update orders set
                __cargo_totals_json__ = cm_merge_json(
            __cargo_totals_json__,
            jsonb_build_object(
                'id', new.id,'id_order', new.id_order,'id_product_type', new.id_product_type,'total_weight', new.total_weight
            ),
            TG_OP
        ),
                (
                    cargos_weight,
                    cargos_products_names
                ) = (
                    select
                            sum(source_row.total_weight) as cargos_weight,
                            string_agg(product_types.name, ', ') as cargos_products_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __cargo_totals_json__,
                jsonb_build_object(
                    'id', new.id,'id_order', new.id_order,'id_product_type', new.id_product_type,'total_weight', new.total_weight
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.cargos, json_entry.value) as record on
                            true
                    ) as source_row

                    left join product_types on
                        product_types.id = source_row.id_product_type
                    where
                        source_row.id_order = orders.id
                )
            where
                new.id_order = orders.id;

            return new;
        end if;

        if old.id_order is not null then
            update orders set
                __cargo_totals_json__ = __cargo_totals_json__ - old.id::text,
                (
                    cargos_weight,
                    cargos_products_names
                ) = (
                    select
                            sum(source_row.total_weight) as cargos_weight,
                            string_agg(product_types.name, ', ') as cargos_products_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    __cargo_totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.cargos, json_entry.value) as record on
                            true
                    ) as source_row

                    left join product_types on
                        product_types.id = source_row.id_product_type
                    where
                        source_row.id_order = orders.id
                )
            where
                old.id_order = orders.id;
        end if;

        if new.id_order is not null then
            update orders set
                __cargo_totals_json__ = cm_merge_json(
            __cargo_totals_json__,
            jsonb_build_object(
                'id', new.id,'id_order', new.id_order,'id_product_type', new.id_product_type,'total_weight', new.total_weight
            ),
            TG_OP
        ),
                (
                    cargos_weight,
                    cargos_products_names
                ) = (
                    select
                            sum(source_row.total_weight) as cargos_weight,
                            string_agg(product_types.name, ', ') as cargos_products_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __cargo_totals_json__,
                jsonb_build_object(
                    'id', new.id,'id_order', new.id_order,'id_product_type', new.id_product_type,'total_weight', new.total_weight
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.cargos, json_entry.value) as record on
                            true
                    ) as source_row

                    left join product_types on
                        product_types.id = source_row.id_product_type
                    where
                        source_row.id_order = orders.id
                )
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_order is not null then
            update orders set
                __cargo_totals_json__ = cm_merge_json(
            __cargo_totals_json__,
            jsonb_build_object(
                'id', new.id,'id_order', new.id_order,'id_product_type', new.id_product_type,'total_weight', new.total_weight
            ),
            TG_OP
        ),
                (
                    cargos_weight,
                    cargos_products_names
                ) = (
                    select
                            sum(source_row.total_weight) as cargos_weight,
                            string_agg(product_types.name, ', ') as cargos_products_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __cargo_totals_json__,
                jsonb_build_object(
                    'id', new.id,'id_order', new.id_order,'id_product_type', new.id_product_type,'total_weight', new.total_weight
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.cargos, json_entry.value) as record on
                            true
                    ) as source_row

                    left join product_types on
                        product_types.id = source_row.id_product_type
                    where
                        source_row.id_order = orders.id
                )
            where
                new.id_order = orders.id;
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