create or replace function cache_totals_for_orders_on_cargos()
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
            select old_row.id, old_row.id_order
            union
            select new_row.id, new_row.id_order
        )
    update orders set
        (
            cars_numbers,
            __totals_json__
        ) = (
            select
                    string_agg(cars.car_number, ', ') as cars_numbers,
                    ('{' || string_agg(
                                                    '"' || public.cargos.id::text || '":' || jsonb_build_object(
                                'id', public.cargos.id,'id_order', public.cargos.id_order
                            )::text,
                                                    ','
                                                ) || '}')
                    ::
                    jsonb as __totals_json__
            from cargos

            inner join cargo_unit_link as link on
                link.id_cargo = cargos.id

            inner join cars on
                cars.id = link.id_car
            where
                cargos.id_order = orders.id
        )
    from changed_rows
    where
        changed_rows.id_order = orders.id;

    return return_row;
end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_cargos
after insert or update of id_order or delete
on public.cargos
for each row
execute procedure cache_totals_for_orders_on_cargos();