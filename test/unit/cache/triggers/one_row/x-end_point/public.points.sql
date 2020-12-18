create or replace function cache_totals_for_orders_on_points()
returns trigger as $body$
declare new_totals record;
begin

    if TG_OP = 'DELETE' then
        if (old.transport_type in ('car', 'truck')) is not true then
            return old;
        end if;

        if old.id_order is null then
            return old;
        end if;

        update orders set
            end_car_point_ids = array_remove(end_car_point_ids, old.id),
            end_car_point_eta = prev_end_car_point.expected_date,
            end_car_point_ata = prev_end_car_point.actual_date
        from points as prev_end_car_point
        where
            orders.id = old.id_order and
            prev_end_car_point.id = (
                select max( prev_id )
                from unnest( array_remove(end_car_point_ids, old.id) ) as prev_id
            );

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.actual_date is not distinct from old.actual_date
            and
            new.expected_date is not distinct from old.expected_date
            and
            new.id_order is not distinct from old.id_order
            and
            new.transport_type is not distinct from old.transport_type
        then
            return new;
        end if;

        if
            old.id_order is null
            and
            new.id_order is null
        then
            return new;
        end if;

        if
            (old.transport_type in ('car', 'truck')) is not true
            and
            (new.transport_type in ('car', 'truck')) is not true
        then
            return new;
        end if;


        if
            new.id_order is not distinct from old.id_order
            and
            (
                (old.transport_type in ('car', 'truck')) is true
                or
                (new.transport_type in ('car', 'truck')) is true
            )
        then
            update orders set
                end_car_point_eta = new.expected_date,
                end_car_point_ata = new.actual_date
            where
                orders.id = new.id_order;
            
            return new;
        end if;


        if
            old.id_order is not null
            and
            (old.transport_type in ('car', 'truck')) is true
            and
            (
                new.id_order is distinct from old.id_order
                or
                (new.transport_type in ('car', 'truck')) is not true
            )
        then
            update orders set
                end_car_point_ids = array_remove(end_car_point_ids, old.id),
                end_car_point_eta = prev_end_car_point.expected_date,
                end_car_point_ata = prev_end_car_point.actual_date
            from points as prev_end_car_point
            where
                orders.id = old.id_order and
                prev_end_car_point.id = (
                    select max( prev_id )
                    from unnest( array_remove(end_car_point_ids, old.id) ) as prev_id
                );
        end if;

        if
            new.id_order is not null
            and
            new.transport_type in ('car', 'truck')
            and
            (
                new.id_order is distinct from old.id_order
                or
                (old.transport_type in ('car', 'truck')) is not true
            )
        then
            update orders set
                end_car_point_ids = array_append(end_car_point_ids, new.id),
                end_car_point_eta = new.expected_date,
                end_car_point_ata = new.actual_date
            where
                orders.id = new.id_order;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if (new.transport_type in ('car', 'truck')) is not true then
            return new;
        end if;

        if new.id_order is null then
            return new;
        end if;

        update orders set
            end_car_point_ids = array_append(end_car_point_ids, new.id),
            end_car_point_eta = new.expected_date,
            end_car_point_ata = new.actual_date
        where
            orders.id = new.id_order;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_points
after insert or update of actual_date, expected_date, id_order, transport_type or delete
on public.points
for each row
execute procedure cache_totals_for_orders_on_points();