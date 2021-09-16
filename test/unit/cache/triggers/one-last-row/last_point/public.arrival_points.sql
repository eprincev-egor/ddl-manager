create or replace function cache_last_point_for_operations_on_arrival_points()
returns trigger as $body$
declare cache_row record;
begin

    if TG_OP = 'DELETE' then
        if old.id_operation is not null then
            select
                operations.id as id,
                operations.__last_point_id as __last_point_id,
                operations.__last_point_sort as __last_point_sort
            from operations
            where
                old.id_operation = operations.id
            for update
            into cache_row;

            if cache_row.__last_point_id = old.id then
                update operations set
                    (
                        __last_point_id,
                        __last_point_sort,
                        last_point_actual_date,
                        last_point_expected_date,
                        last_point_id_point
                    ) = (
                        select
                            arrival_points.id as __last_point_id,
                            arrival_points.sort as __last_point_sort,
                arrival_points.actual_date as last_point_actual_date,
                arrival_points.expected_date as last_point_expected_date,
                arrival_points.id_point as last_point_id_point

                        from arrival_points

                        where
                            arrival_points.id_operation = operations.id
            order by
                arrival_points.sort desc nulls first,
                public.arrival_points.id desc nulls first
            limit 1
                    )
                where
                    operations.id = cache_row.id;
            end if;
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

        if new.id_operation is not distinct from old.id_operation then
            if new.id_operation is null then
                return new;
            end if;

            if new.sort is distinct from old.sort then
                select
                    operations.id as id,
                    operations.__last_point_id as __last_point_id,
                    operations.__last_point_sort as __last_point_sort
                from operations
                where
                    old.id_operation = operations.id
                for update
                into cache_row;

                if
                    new.sort is not distinct from old.sort
                    and
                    new.id > old.id
                    or
                    new.sort is null
                    and
                    old.sort is not null
                    or
                    new.sort > old.sort
                then
                    if
                        new.sort is not distinct from cache_row.__last_point_sort
                        and
                        new.id > cache_row.__last_point_id
                        or
                        new.sort is null
                        and
                        cache_row.__last_point_sort is not null
                        or
                        new.sort > cache_row.__last_point_sort
                    then
                        update operations set
                            __last_point_id = new.id,
                            __last_point_sort = new.sort,
                            last_point_actual_date = new.actual_date,
                            last_point_expected_date = new.expected_date,
                            last_point_id_point = new.id_point
                        where
                            new.id_operation = operations.id;
                    end if;
                else
                    if cache_row.__last_point_id = new.id then
                        update operations set
                            (
                                __last_point_id,
                                __last_point_sort,
                                last_point_actual_date,
                                last_point_expected_date,
                                last_point_id_point
                            ) = (
                                select
                                    arrival_points.id as __last_point_id,
                                    arrival_points.sort as __last_point_sort,
                arrival_points.actual_date as last_point_actual_date,
                arrival_points.expected_date as last_point_expected_date,
                arrival_points.id_point as last_point_id_point

                                from arrival_points

                                where
                                    arrival_points.id_operation = operations.id
            order by
                arrival_points.sort desc nulls first,
                public.arrival_points.id desc nulls first
            limit 1
                            )
                        where
                            operations.id = cache_row.id;
                    end if;
                end if;
            else
                update operations set
                    last_point_actual_date = new.actual_date,
                    last_point_expected_date = new.expected_date,
                    last_point_id_point = new.id_point
                where
                    new.id_operation = operations.id
                    and
                    operations.__last_point_id = new.id
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

        if old.id_operation is not null then
            select
                operations.id as id,
                operations.__last_point_id as __last_point_id,
                operations.__last_point_sort as __last_point_sort
            from operations
            where
                old.id_operation = operations.id
            for update
            into cache_row;

            if cache_row.__last_point_id = old.id then
                update operations set
                    (
                        __last_point_id,
                        __last_point_sort,
                        last_point_actual_date,
                        last_point_expected_date,
                        last_point_id_point
                    ) = (
                        select
                            arrival_points.id as __last_point_id,
                            arrival_points.sort as __last_point_sort,
                arrival_points.actual_date as last_point_actual_date,
                arrival_points.expected_date as last_point_expected_date,
                arrival_points.id_point as last_point_id_point

                        from arrival_points

                        where
                            arrival_points.id_operation = operations.id
            order by
                arrival_points.sort desc nulls first,
                public.arrival_points.id desc nulls first
            limit 1
                    )
                where
                    operations.id = cache_row.id;
            end if;
        end if;

        if new.id_operation is not null then
            select
                operations.id as id,
                operations.__last_point_id as __last_point_id,
                operations.__last_point_sort as __last_point_sort
            from operations
            where
                new.id_operation = operations.id
            for update
            into cache_row;

            update operations set
                __last_point_id = new.id,
                __last_point_sort = new.sort,
                last_point_actual_date = new.actual_date,
                last_point_expected_date = new.expected_date,
                last_point_id_point = new.id_point
            where
                new.id_operation = operations.id
                and
                (
                    operations.__last_point_id is null
                    or
                    operations.__last_point_sort is not distinct from new.sort
                    and
                    operations.__last_point_id < new.id
                    or
                    operations.__last_point_sort is not null
                    and
                    new.sort is null
                    or
                    operations.__last_point_sort < new.sort
                );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if new.id_operation is not null then
            select
                operations.id as id,
                operations.__last_point_id as __last_point_id,
                operations.__last_point_sort as __last_point_sort
            from operations
            where
                new.id_operation = operations.id
            for update
            into cache_row;

            update operations set
                __last_point_id = new.id,
                __last_point_sort = new.sort,
                last_point_actual_date = new.actual_date,
                last_point_expected_date = new.expected_date,
                last_point_id_point = new.id_point
            where
                new.id_operation = operations.id
                and
                (
                    operations.__last_point_id is null
                    or
                    operations.__last_point_sort is not distinct from new.sort
                    and
                    operations.__last_point_id < new.id
                    or
                    operations.__last_point_sort is not null
                    and
                    new.sort is null
                    or
                    operations.__last_point_sort < new.sort
                );
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