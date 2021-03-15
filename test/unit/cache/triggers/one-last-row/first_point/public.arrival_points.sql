create or replace function cache_first_point_for_operations_on_arrival_points()
returns trigger as $body$
declare prev_row record;
begin

    if TG_OP = 'DELETE' then
        if
            old.id_operation is not null
            and
            old.deleted = 0
        then
            if not old.__first_point_for_operations then
                return old;
            end if;

            select
                id,
                actual_date,
                deleted,
                expected_date,
                id_operation,
                id_point,
                sort
            from arrival_points as first_point
            where
                first_point.id_operation = old.id_operation
                and
                first_point.deleted = 0
            order by
                first_point.sort asc nulls first
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update arrival_points as first_point set
                    __first_point_for_operations = true
                where
                    first_point.id = prev_row.id;
            end if;

            update operations set
                first_point_actual_date = prev_row.actual_date,
                first_point_expected_date = prev_row.expected_date,
                first_point_id_point = prev_row.id_point
            where
                old.id_operation = operations.id
                and
                (
                    operations.first_point_actual_date is distinct from prev_row.actual_date
                    or
                    operations.first_point_expected_date is distinct from prev_row.expected_date
                    or
                    operations.first_point_id_point is distinct from prev_row.id_point
                );

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
            and
            new.sort is not distinct from old.sort
        then
            if
                new.id_operation is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            if not new.__first_point_for_operations then
                return new;
            end if;

            update operations set
                first_point_actual_date = new.actual_date,
                first_point_expected_date = new.expected_date,
                first_point_id_point = new.id_point
            where
                new.id_operation = operations.id
                and
                (
                    operations.first_point_actual_date is distinct from new.actual_date
                    or
                    operations.first_point_expected_date is distinct from new.expected_date
                    or
                    operations.first_point_id_point is distinct from new.id_point
                );

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

            if
                not new.__first_point_for_operations
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
                    sort
                from arrival_points as first_point
                where
                    first_point.id_operation = new.id_operation
                    and
                    first_point.deleted = 0
                    and
                    first_point.__first_point_for_operations = true
                into prev_row;

                if
                    prev_row.id is null
                    or
                    prev_row.sort is not null
                    and
                    new.sort is null
                    or
                    prev_row.sort > new.sort
                then
                    update arrival_points as first_point set
                        __first_point_for_operations = (first_point.id = new.id)
                    where
                        first_point.id in (new.id, prev_row.id);

                    update operations set
                        first_point_actual_date = new.actual_date,
                        first_point_expected_date = new.expected_date,
                        first_point_id_point = new.id_point
                    where
                        new.id_operation = operations.id
                        and
                        (
                            operations.first_point_actual_date is distinct from new.actual_date
                            or
                            operations.first_point_expected_date is distinct from new.expected_date
                            or
                            operations.first_point_id_point is distinct from new.id_point
                        );

                    return new;
                end if;
            end if;

            if
                new.__first_point_for_operations
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
                    actual_date,
                    deleted,
                    expected_date,
                    id_operation,
                    id_point,
                    sort
                from arrival_points as first_point
                where
                    first_point.id_operation = new.id_operation
                    and
                    first_point.deleted = 0
                    and
                    (
                        first_point.sort is null
                        and
                        new.sort is not null
                        or
                        first_point.sort < new.sort
                    )
                    and
                    first_point.id <> new.id
                order by
                    first_point.sort asc nulls first
                limit 1
                into prev_row;

                if
                    prev_row.id is not null
                    and
                    (
                        prev_row.sort is null
                        and
                        new.sort is not null
                        or
                        prev_row.sort < new.sort
                    )
                then
                    update arrival_points as first_point set
                        __first_point_for_operations = (first_point.id != new.id)
                    where
                        first_point.id in (new.id, prev_row.id);

                    update operations set
                        first_point_actual_date = prev_row.actual_date,
                        first_point_expected_date = prev_row.expected_date,
                        first_point_id_point = prev_row.id_point
                    where
                        old.id_operation = operations.id
                        and
                        (
                            operations.first_point_actual_date is distinct from prev_row.actual_date
                            or
                            operations.first_point_expected_date is distinct from prev_row.expected_date
                            or
                            operations.first_point_id_point is distinct from prev_row.id_point
                        );

                    return new;
                end if;
            end if;

            if
                new.__first_point_for_operations
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
                    first_point_actual_date = new.actual_date,
                    first_point_expected_date = new.expected_date,
                    first_point_id_point = new.id_point
                where
                    new.id_operation = operations.id
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
            and
            old.__first_point_for_operations
        then
            select
                id,
                actual_date,
                deleted,
                expected_date,
                id_operation,
                id_point,
                sort
            from arrival_points as first_point
            where
                first_point.id_operation = old.id_operation
                and
                first_point.deleted = 0
            order by
                first_point.sort asc nulls first
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update arrival_points as first_point set
                    __first_point_for_operations = true
                where
                    first_point.id = prev_row.id;
            end if;

            if
                new.id_operation is null
                or
                not coalesce(new.deleted = 0, false)
            then
                update arrival_points as first_point set
                    __first_point_for_operations = false
                where
                    first_point.id = new.id;
            end if;

            update operations set
                first_point_actual_date = prev_row.actual_date,
                first_point_expected_date = prev_row.expected_date,
                first_point_id_point = prev_row.id_point
            where
                old.id_operation = operations.id
                and
                (
                    operations.first_point_actual_date is distinct from prev_row.actual_date
                    or
                    operations.first_point_expected_date is distinct from prev_row.expected_date
                    or
                    operations.first_point_id_point is distinct from prev_row.id_point
                );
        end if;

        if
            new.id_operation is not null
            and
            new.deleted = 0
        then
            select
                id,
                sort
            from arrival_points as first_point
            where
                first_point.id_operation = new.id_operation
                and
                first_point.deleted = 0
                and
                first_point.__first_point_for_operations = true
            into prev_row;

            if
                prev_row.id is null
                or
                prev_row.sort is not null
                and
                new.sort is null
                or
                prev_row.sort > new.sort
            then
                if prev_row.id is not null then
                    update arrival_points as first_point set
                        __first_point_for_operations = false
                    where
                        first_point.id = prev_row.id
                        and
                        __first_point_for_operations = true;
                end if;

                if not new.__first_point_for_operations then
                    update arrival_points as first_point set
                        __first_point_for_operations = true
                    where
                        first_point.id = new.id;
                end if;

                update operations set
                    first_point_actual_date = new.actual_date,
                    first_point_expected_date = new.expected_date,
                    first_point_id_point = new.id_point
                where
                    new.id_operation = operations.id
                    and
                    (
                        operations.first_point_actual_date is distinct from new.actual_date
                        or
                        operations.first_point_expected_date is distinct from new.expected_date
                        or
                        operations.first_point_id_point is distinct from new.id_point
                    );
            end if;
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
                id,
                sort
            from arrival_points as first_point
            where
                first_point.id_operation = new.id_operation
                and
                first_point.deleted = 0
                and
                first_point.__first_point_for_operations = true
            into prev_row;

            if
                prev_row.id is null
                or
                prev_row.sort is not null
                and
                new.sort is null
                or
                prev_row.sort > new.sort
            then
                update arrival_points as first_point set
                    __first_point_for_operations = (first_point.id = new.id)
                where
                    first_point.id in (new.id, prev_row.id);

                update operations set
                    first_point_actual_date = new.actual_date,
                    first_point_expected_date = new.expected_date,
                    first_point_id_point = new.id_point
                where
                    new.id_operation = operations.id
                    and
                    (
                        operations.first_point_actual_date is distinct from new.actual_date
                        or
                        operations.first_point_expected_date is distinct from new.expected_date
                        or
                        operations.first_point_id_point is distinct from new.id_point
                    );
            end if;
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