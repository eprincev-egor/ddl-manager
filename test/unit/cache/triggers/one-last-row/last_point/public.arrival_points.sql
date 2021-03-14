create or replace function cache_last_point_for_operations_on_arrival_points()
returns trigger as $body$
declare prev_row record;
begin

    if TG_OP = 'DELETE' then
        if old.id_operation is not null then
            if not old.__last_point_for_operations then
                return old;
            end if;

            select
                id,
                actual_date,
                expected_date,
                id_operation,
                id_point,
                sort
            from arrival_points
            where
                arrival_points.id_operation = old.id_operation
            order by
                arrival_points.sort desc nulls last
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update arrival_points set
                    __last_point_for_operations = true
                where
                    arrival_points.id = prev_row.id;
            end if;

            update operations set
                last_point_actual_date = prev_row.actual_date,
                last_point_expected_date = prev_row.expected_date,
                last_point_id_point = prev_row.id_point
            where
                old.id_operation = operations.id
                and
                (
                    operations.last_point_actual_date is distinct from prev_row.actual_date
                    or
                    operations.last_point_expected_date is distinct from prev_row.expected_date
                    or
                    operations.last_point_id_point is distinct from prev_row.id_point
                );

        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.actual_date is not distinct from old.actual_date
            and
            new.expected_date is not distinct from old.expected_date
            and
            new.id_operation is not distinct from old.id_operation
            and
            new.id_point is not distinct from old.id_point
            and
            new.sort is not distinct from old.sort
        then
            return new;
        end if;

        if
            new.id_operation is not distinct from old.id_operation
            and
            new.sort is not distinct from old.sort
        then
            if new.id_operation is null then
                return new;
            end if;

            if not new.__last_point_for_operations then
                return new;
            end if;

            update operations set
                last_point_actual_date = new.actual_date,
                last_point_expected_date = new.expected_date,
                last_point_id_point = new.id_point
            where
                new.id_operation = operations.id
                and
                (
                    operations.last_point_actual_date is distinct from new.actual_date
                    or
                    operations.last_point_expected_date is distinct from new.expected_date
                    or
                    operations.last_point_id_point is distinct from new.id_point
                );

            return new;
        end if;

        if new.id_operation is not distinct from old.id_operation then
            if
                not new.__last_point_for_operations
                and
                (
                    new.sort is not null
                    and
                    old.sort is null
                    or
                    new.sort > old.sort
                )
            then
                select
                    id,
                    sort
                from arrival_points
                where
                    arrival_points.id_operation = new.id_operation
                    and
                    arrival_points.__last_point_for_operations = true
                into prev_row;

                if
                    prev_row.id is null
                    or
                    prev_row.sort is null
                    and
                    new.sort is not null
                    or
                    prev_row.sort < new.sort
                then
                    update arrival_points set
                        __last_point_for_operations = (arrival_points.id = new.id)
                    where
                        arrival_points.id in (new.id, prev_row.id);

                    update operations set
                        last_point_actual_date = new.actual_date,
                        last_point_expected_date = new.expected_date,
                        last_point_id_point = new.id_point
                    where
                        new.id_operation = operations.id
                        and
                        (
                            operations.last_point_actual_date is distinct from new.actual_date
                            or
                            operations.last_point_expected_date is distinct from new.expected_date
                            or
                            operations.last_point_id_point is distinct from new.id_point
                        );

                    return new;
                end if;
            end if;

            if
                new.__last_point_for_operations
                and
                (
                    new.sort is null
                    and
                    old.sort is not null
                    or
                    new.sort < old.sort
                )
            then
                select
                    id,
                    actual_date,
                    expected_date,
                    id_operation,
                    id_point,
                    sort
                from arrival_points
                where
                    arrival_points.id_operation = new.id_operation
                    and
                    arrival_points.sort > new.sort
                order by
                    arrival_points.sort desc nulls last
                limit 1
                into prev_row;

                if
                    prev_row.sort is not null
                    and
                    new.sort is null
                    or
                    prev_row.sort > new.sort
                then
                    update arrival_points set
                        __last_point_for_operations = (arrival_points.id != new.id)
                    where
                        arrival_points.id in (new.id, prev_row.id);

                    update operations set
                        last_point_actual_date = prev_row.actual_date,
                        last_point_expected_date = prev_row.expected_date,
                        last_point_id_point = prev_row.id_point
                    where
                        old.id_operation = operations.id
                        and
                        (
                            operations.last_point_actual_date is distinct from prev_row.actual_date
                            or
                            operations.last_point_expected_date is distinct from prev_row.expected_date
                            or
                            operations.last_point_id_point is distinct from prev_row.id_point
                        );

                    return new;
                end if;
            end if;

            if
                new.__last_point_for_operations
                and
                (
                    new.actual_date is distinct from old.actual_date
                    or
                    new.expected_date is distinct from old.expected_date
                    or
                    new.id_point is distinct from old.id_point
                )
            then
                update operations set
                    last_point_actual_date = new.actual_date,
                    last_point_expected_date = new.expected_date,
                    last_point_id_point = new.id_point
                where
                    new.id_operation = operations.id
                    and
                    (
                        operations.last_point_actual_date is distinct from new.actual_date
                        or
                        operations.last_point_expected_date is distinct from new.expected_date
                        or
                        operations.last_point_id_point is distinct from new.id_point
                    );
            end if;

            return new;
        end if;

        if
            old.id_operation is not null
            and
            old.__last_point_for_operations
        then
            select
                id,
                actual_date,
                expected_date,
                id_operation,
                id_point,
                sort
            from arrival_points
            where
                arrival_points.id_operation = old.id_operation
            order by
                arrival_points.sort desc nulls last
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update arrival_points set
                    __last_point_for_operations = true
                where
                    arrival_points.id = prev_row.id;
            end if;

            update operations set
                last_point_actual_date = prev_row.actual_date,
                last_point_expected_date = prev_row.expected_date,
                last_point_id_point = prev_row.id_point
            where
                old.id_operation = operations.id
                and
                (
                    operations.last_point_actual_date is distinct from prev_row.actual_date
                    or
                    operations.last_point_expected_date is distinct from prev_row.expected_date
                    or
                    operations.last_point_id_point is distinct from prev_row.id_point
                );
        end if;

        if new.id_operation is not null then
            select
                id,
                sort
            from arrival_points
            where
                arrival_points.id_operation = new.id_operation
                and
                arrival_points.__last_point_for_operations = true
            into prev_row;

            if
                prev_row.id is null
                or
                prev_row.sort is null
                and
                new.sort is not null
                or
                prev_row.sort < new.sort
            then
                if prev_row.id is not null then
                    update arrival_points set
                        __last_point_for_operations = false
                    where
                        arrival_points.id = prev_row.id
                        and
                        __last_point_for_operations = true;
                end if;

                if not new.__last_point_for_operations then
                    update arrival_points set
                        __last_point_for_operations = true
                    where
                        arrival_points.id = new.id;
                end if;

                update operations set
                    last_point_actual_date = new.actual_date,
                    last_point_expected_date = new.expected_date,
                    last_point_id_point = new.id_point
                where
                    new.id_operation = operations.id
                    and
                    (
                        operations.last_point_actual_date is distinct from new.actual_date
                        or
                        operations.last_point_expected_date is distinct from new.expected_date
                        or
                        operations.last_point_id_point is distinct from new.id_point
                    );
            end if;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if new.id_operation is not null then

            select
                id,
                sort
            from arrival_points
            where
                arrival_points.id_operation = new.id_operation
                and
                arrival_points.__last_point_for_operations = true
            into prev_row;

            if
                prev_row.id is null
                or
                prev_row.sort is null
                and
                new.sort is not null
                or
                prev_row.sort < new.sort
            then
                update arrival_points set
                    __last_point_for_operations = (arrival_points.id = new.id)
                where
                    arrival_points.id in (new.id, prev_row.id);

                update operations set
                    last_point_actual_date = new.actual_date,
                    last_point_expected_date = new.expected_date,
                    last_point_id_point = new.id_point
                where
                    new.id_operation = operations.id
                    and
                    (
                        operations.last_point_actual_date is distinct from new.actual_date
                        or
                        operations.last_point_expected_date is distinct from new.expected_date
                        or
                        operations.last_point_id_point is distinct from new.id_point
                    );
            end if;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_last_point_for_operations_on_arrival_points
after insert or update of actual_date, expected_date, id_operation, id_point, sort or delete
on public.arrival_points
for each row
execute procedure cache_last_point_for_operations_on_arrival_points();