create or replace function cache_cargo_totals_for_orders_on_product_types()
returns trigger as $body$
declare new_row record;
declare old_row record;
declare return_row record;
begin
    if TG_OP = 'DELETE' then
        return_row = old;
    else
        return_row = new;
    end if;

    new_row = return_row;
    old_row = return_row;

    if TG_OP in ('INSERT', 'UPDATE') then
        new_row = new;
    end if;
    if TG_OP in ('UPDATE', 'DELETE') then
        old_row = old;
    end if;

    with
        changed_rows as (
            select old_row.id, old_row.name
            union
            select new_row.id, new_row.name
        )
    update orders set
        (
            cargos_weight,
            cargos_products_names_name,
            cargos_products_names
        ) = (
            select
                coalesce(
        sum(cargos.total_weight),
        0
    ) as cargos_weight,
                array_agg(product_types.name) as cargos_products_names_name,
    string_agg(product_types.name, ', ') as cargos_products_names

            from cargos

            left join product_types on
                product_types.id = cargos.id_product_type

            where
                cargos.id_order = orders.id
        )
    from changed_rows, cargos
    where
        cargos.id_order = orders.id
        and
        changed_rows.id = cargos.id_product_type;

    return return_row;
end
$body$
language plpgsql;

create trigger cache_cargo_totals_for_orders_on_product_types
after insert or update of name or delete
on public.product_types
for each row
execute procedure cache_cargo_totals_for_orders_on_product_types();