create or replace function cache_totals_for_orders_on_cargos()
returns trigger as $body$
declare old_car_car_number text;
declare new_car_car_number text;
begin

    if TG_OP = 'DELETE' then

        if old.id_order is not null then
            if old.id_car is not null then
                old_car_car_number = (
                    select
                        cars.car_number
                    from cars
                    where
                        cars.id = old.id_car
                );
            end if;

            update orders set
                cars_numbers_car_number = cm_array_remove_one_element(
                    cars_numbers_car_number,
                    old_car_car_number
                ),
                cars_numbers = (
                    select
                        string_agg(item.car_number, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            cars_numbers_car_number,
                            old_car_car_number
                        )
                    ) as item(car_number)
                )
            where
                old.id_order = orders.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.id_order is not distinct from old.id_order then
            return new;
        end if;

        if old.id_car is not null then
            old_car_car_number = (
                select
                    cars.car_number
                from cars
                where
                    cars.id = old.id_car
            );
        end if;

        if new.id_car is not distinct from old.id_car then
            new_car_car_number = old_car_car_number;
        else
            if new.id_car is not null then
                new_car_car_number = (
                    select
                        cars.car_number
                    from cars
                    where
                        cars.id = new.id_car
                );
            end if;
        end if;

        if new.id_order is not distinct from old.id_order then
            update orders set
                cars_numbers_car_number = array_append(
                    cm_array_remove_one_element(
                        cars_numbers_car_number,
                        old_car_car_number
                    ),
                    new_car_car_number
                ),
                cars_numbers = (
                    select
                        string_agg(item.car_number, ', ')

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                cars_numbers_car_number,
                                old_car_car_number
                            ),
                            new_car_car_number
                        )
                    ) as item(car_number)
                )
            where
                new.id_order = orders.id;

            return new;
        end if;

        if old.id_order is not null then
            update orders set
                cars_numbers_car_number = cm_array_remove_one_element(
                    cars_numbers_car_number,
                    old_car_car_number
                ),
                cars_numbers = (
                    select
                        string_agg(item.car_number, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            cars_numbers_car_number,
                            old_car_car_number
                        )
                    ) as item(car_number)
                )
            where
                old.id_order = orders.id;
        end if;

        if new.id_order is not null then
            update orders set
                cars_numbers_car_number = array_append(
                    cars_numbers_car_number,
                    new_car_car_number
                ),
                cars_numbers = coalesce(
                    cars_numbers ||
                    coalesce(
                        ', '
                        || new_car_car_number,
                        ''
                    ),
                    new_car_car_number
                )
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_order is not null then
            if new.id_car is not null then
                new_car_car_number = (
                    select
                        cars.car_number
                    from cars
                    where
                        cars.id = new.id_car
                );
            end if;

            update orders set
                cars_numbers_car_number = array_append(
                    cars_numbers_car_number,
                    new_car_car_number
                ),
                cars_numbers = coalesce(
                    cars_numbers ||
                    coalesce(
                        ', '
                        || new_car_car_number,
                        ''
                    ),
                    new_car_car_number
                )
            where
                new.id_order = orders.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_cargos
after insert or update of id_order or delete
on public.cargos
for each row
execute procedure cache_totals_for_orders_on_cargos();