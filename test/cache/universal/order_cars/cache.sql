cache totals for orders (
    select 
        string_agg( cars.car_number, ', ' ) as cars_numbers
    from cargos

    inner join cargo_unit_link as link on
        link.id_cargo = cargos.id
    
    inner join cars on
        cars.id = link.id_car

    where
        cargos.id_order = orders.id
)