create or replace function cache_totals_for_orders_on_cars()
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
            select old_row.car_number, old_row.id
            union
            select new_row.car_number, new_row.id
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
    from changed_rows, cargo_unit_link as link, cargos
    where
        cargos.id_order = orders.id
        and
        link.id_cargo = cargos.id
        and
        changed_rows.id = link.id_car;

    return return_row;
end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_cars
after insert or update of car_number or delete
on public.cars
for each row
execute procedure cache_totals_for_orders_on_cars();