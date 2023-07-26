create or replace function cache_first_point_for_operations_on_arrival_points()
returns trigger as $body$
declare cache_row record;
begin

    if TG_OP = 'DELETE' then
        if
            old.id_operation is not null
            and
            old.deleted = 0
        then
            select
                    operations.id as id,
                    operations.__first_point_sort as __first_point_sort,
                    operations.__first_point_id as __first_point_id
            from operations
            where
                old.id_operation = operations.id
            for update
            into cache_row;

            if cache_row.__first_point_id = old.id then
                update operations set
                    (
                        __first_point_sort,
                        __first_point_id,
                        first_point_actual_date,
                        first_point_expected_date,
                        first_point_id_point
                    ) = (
                        select
                                first_point.sort as __first_point_sort,
                                first_point.id as __first_point_id,
                                first_point.actual_date as first_point_actual_date,
                                first_point.expected_date as first_point_expected_date,
                                first_point.id_point as first_point_id_point
                        from arrival_points as first_point
                        where
                            first_point.id_operation = operations.id
                            and
                            first_point.deleted = 0
                        order by
                            first_point.sort asc nulls last,
                            first_point.id asc nulls last
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
            new.deleted is not distinct from old.deleted
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
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_operation is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            if new.sort is distinct from old.sort then
                select
                        operations.id as id,
                        operations.__first_point_sort as __first_point_sort,
                        operations.__first_point_id as __first_point_id
                from operations
                where
                    old.id_operation = operations.id
                for update
                into cache_row;

                if
                    new.sort is not distinct from old.sort
                    and
                    new.id < old.id
                    or
                    new.sort is not null
                    and
                    old.sort is null
                    or
                    new.sort < old.sort
                then
                    if
                        new.sort is not distinct from cache_row.__first_point_sort
                        and
                        new.id < cache_row.__first_point_id
                        or
                        new.sort is not null
                        and
                        cache_row.__first_point_sort is null
                        or
                        new.sort < cache_row.__first_point_sort
                    then
                        update operations set
                            __first_point_sort = new.sort,
                            __first_point_id = new.id,
                            first_point_actual_date = new.actual_date,
                            first_point_expected_date = new.expected_date,
                            first_point_id_point = new.id_point
                        where
                            new.id_operation = operations.id;
                    end if;
                else
                    if cache_row.__first_point_id = new.id then
                        update operations set
                            (
                                __first_point_sort,
                                __first_point_id,
                                first_point_actual_date,
                                first_point_expected_date,
                                first_point_id_point
                            ) = (
                                select
                                        first_point.sort as __first_point_sort,
                                        first_point.id as __first_point_id,
                                        first_point.actual_date as first_point_actual_date,
                                        first_point.expected_date as first_point_expected_date,
                                        first_point.id_point as first_point_id_point
                                from arrival_points as first_point
                                where
                                    first_point.id_operation = operations.id
                                    and
                                    first_point.deleted = 0
                                order by
                                    first_point.sort asc nulls last,
                                    first_point.id asc nulls last
                                limit 1
                            )
                        where
                            operations.id = cache_row.id;
                    end if;
                end if;
            else
                update operations set
                    first_point_actual_date = new.actual_date,
                    first_point_expected_date = new.expected_date,
                    first_point_id_point = new.id_point
                where
                    new.id_operation = operations.id
                    and
                    operations.__first_point_id = new.id
                    and
                    (
                        operations.first_point_actual_date is distinct from new.actual_date
                        or
                        operations.first_point_expected_date is distinct from new.expected_date
                        or
                        operations.first_point_id_point is distinct from new.id_point
                    );
            end if;

            return new;
        end if;

        if
            old.id_operation is not null
            and
            old.deleted = 0
        then
            select
                    operations.id as id,
                    operations.__first_point_sort as __first_point_sort,
                    operations.__first_point_id as __first_point_id
            from operations
            where
                old.id_operation = operations.id
            for update
            into cache_row;

            if cache_row.__first_point_id = old.id then
                update operations set
                    (
                        __first_point_sort,
                        __first_point_id,
                        first_point_actual_date,
                        first_point_expected_date,
                        first_point_id_point
                    ) = (
                        select
                                first_point.sort as __first_point_sort,
                                first_point.id as __first_point_id,
                                first_point.actual_date as first_point_actual_date,
                                first_point.expected_date as first_point_expected_date,
                                first_point.id_point as first_point_id_point
                        from arrival_points as first_point
                        where
                            first_point.id_operation = operations.id
                            and
                            first_point.deleted = 0
                        order by
                            first_point.sort asc nulls last,
                            first_point.id asc nulls last
                        limit 1
                    )
                where
                    operations.id = cache_row.id;
            end if;
        end if;

        if
            new.id_operation is not null
            and
            new.deleted = 0
        then
            select
                    operations.id as id,
                    operations.__first_point_sort as __first_point_sort,
                    operations.__first_point_id as __first_point_id
            from operations
            where
                new.id_operation = operations.id
            for update
            into cache_row;

            update operations set
                __first_point_sort = new.sort,
                __first_point_id = new.id,
                first_point_actual_date = new.actual_date,
                first_point_expected_date = new.expected_date,
                first_point_id_point = new.id_point
            where
                new.id_operation = operations.id
                and
                (
                    operations.__first_point_id is null
                    or
                    operations.__first_point_sort is not distinct from new.sort
                    and
                    operations.__first_point_id > new.id
                    or
                    operations.__first_point_sort is null
                    and
                    new.sort is not null
                    or
                    operations.__first_point_sort > new.sort
                );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.id_operation is not null
            and
            new.deleted = 0
        then
            select
                    operations.id as id,
                    operations.__first_point_sort as __first_point_sort,
                    operations.__first_point_id as __first_point_id
            from operations
            where
                new.id_operation = operations.id
            for update
            into cache_row;

            update operations set
                __first_point_sort = new.sort,
                __first_point_id = new.id,
                first_point_actual_date = new.actual_date,
                first_point_expected_date = new.expected_date,
                first_point_id_point = new.id_point
            where
                new.id_operation = operations.id
                and
                (
                    operations.__first_point_id is null
                    or
                    operations.__first_point_sort is not distinct from new.sort
                    and
                    operations.__first_point_id > new.id
                    or
                    operations.__first_point_sort is null
                    and
                    new.sort is not null
                    or
                    operations.__first_point_sort > new.sort
                );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_first_point_for_operations_on_arrival_points
after insert or update of actual_date, deleted, expected_date, id_operation, id_point, sort or delete
on public.arrival_points
for each row
execute procedure cache_first_point_for_operations_on_arrival_points();