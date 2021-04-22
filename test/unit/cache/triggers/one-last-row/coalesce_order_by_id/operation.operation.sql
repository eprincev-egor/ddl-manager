create or replace function cache_border_crossing_for_order_on_operation()
returns trigger as $body$
declare prev_row record;
declare prev_id bigint;
begin

    if TG_OP = 'DELETE' then
        if
            old.id_order is not null
            and
            old.is_border_crossing = 1
            and
            old.id_doc_parent_operation is null
            and
            old.deleted = 0
        then
            if not old.__border_crossing_for_order then
                return old;
            end if;

            select
                deleted,
                end_expected_date,
                id,
                id_doc_parent_operation,
                id_order,
                is_border_crossing
            from operation.operation as border_crossing
            where
                border_crossing.id_order = old.id_order
                and
                border_crossing.is_border_crossing = 1
                and
                border_crossing.id_doc_parent_operation is null
                and
                border_crossing.deleted = 0
            order by
                border_crossing.id desc nulls first
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update operation.operation as border_crossing set
                    __border_crossing_for_order = true
                where
                    border_crossing.id = prev_row.id;
            end if;

            update public.order as orders set
                id_border_crossing = prev_row.id,
                date_delivery = coalesce(
                    prev_row.end_expected_date,
                    orders.date_delivery
                )
            where
                old.id_order = orders.id
                and
                (
                    orders.id_border_crossing is distinct from prev_row.id
                    or
                    orders.date_delivery is distinct from coalesce(
                        prev_row.end_expected_date,
                        orders.date_delivery
                    )
                );

        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.end_expected_date is not distinct from old.end_expected_date
            and
            new.id_doc_parent_operation is not distinct from old.id_doc_parent_operation
            and
            new.id_order is not distinct from old.id_order
            and
            new.is_border_crossing is not distinct from old.is_border_crossing
        then
            return new;
        end if;

        if
            new.id_order is not distinct from old.id_order
            and
            new.is_border_crossing is not distinct from old.is_border_crossing
            and
            new.id_doc_parent_operation is not distinct from old.id_doc_parent_operation
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_order is null
                or
                not coalesce(new.is_border_crossing = 1, false)
                or
                not coalesce(new.id_doc_parent_operation is null, false)
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            if not new.__border_crossing_for_order then
                return new;
            end if;

            update public.order as orders set
                id_border_crossing = new.id,
                date_delivery = coalesce(
                    new.end_expected_date,
                    orders.date_delivery
                )
            where
                new.id_order = orders.id
                and
                (
                    orders.id_border_crossing is distinct from new.id
                    or
                    orders.date_delivery is distinct from coalesce(
                        new.end_expected_date,
                        orders.date_delivery
                    )
                );

            return new;
        end if;

        if
            old.id_order is not null
            and
            old.is_border_crossing = 1
            and
            old.id_doc_parent_operation is null
            and
            old.deleted = 0
            and
            old.__border_crossing_for_order
        then
            select
                deleted,
                end_expected_date,
                id,
                id_doc_parent_operation,
                id_order,
                is_border_crossing
            from operation.operation as border_crossing
            where
                border_crossing.id_order = old.id_order
                and
                border_crossing.is_border_crossing = 1
                and
                border_crossing.id_doc_parent_operation is null
                and
                border_crossing.deleted = 0
            order by
                border_crossing.id desc nulls first
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update operation.operation as border_crossing set
                    __border_crossing_for_order = true
                where
                    border_crossing.id = prev_row.id;
            end if;

            update public.order as orders set
                id_border_crossing = prev_row.id,
                date_delivery = coalesce(
                    prev_row.end_expected_date,
                    orders.date_delivery
                )
            where
                old.id_order = orders.id
                and
                (
                    orders.id_border_crossing is distinct from prev_row.id
                    or
                    orders.date_delivery is distinct from coalesce(
                        prev_row.end_expected_date,
                        orders.date_delivery
                    )
                );
        end if;

        if
            new.id_order is not null
            and
            new.is_border_crossing = 1
            and
            new.id_doc_parent_operation is null
            and
            new.deleted = 0
        then
            prev_id = (
                select
                    max( border_crossing.id )
                from operation.operation as border_crossing
                where
                    border_crossing.id_order = new.id_order
                    and
                    border_crossing.is_border_crossing = 1
                    and
                    border_crossing.id_doc_parent_operation is null
                    and
                    border_crossing.deleted = 0
                    and
                    border_crossing.id <> new.id
            );

            if
                prev_id < new.id
                or
                prev_id is null
            then
                if prev_id is not null then
                    update operation.operation as border_crossing set
                        __border_crossing_for_order = false
                    where
                        border_crossing.id = prev_id
                        and
                        __border_crossing_for_order = true;
                end if;

                if not new.__border_crossing_for_order then
                    update operation.operation as border_crossing set
                        __border_crossing_for_order = true
                    where
                        border_crossing.id = new.id;
                end if;

                update public.order as orders set
                    id_border_crossing = new.id,
                    date_delivery = coalesce(
                        new.end_expected_date,
                        orders.date_delivery
                    )
                where
                    new.id_order = orders.id
                    and
                    (
                        orders.id_border_crossing is distinct from new.id
                        or
                        orders.date_delivery is distinct from coalesce(
                            new.end_expected_date,
                            orders.date_delivery
                        )
                    );
            end if;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.id_order is not null
            and
            new.is_border_crossing = 1
            and
            new.id_doc_parent_operation is null
            and
            new.deleted = 0
        then
            update operation.operation as border_crossing set
                __border_crossing_for_order = false
            where
                border_crossing.id_order = new.id_order
                and
                border_crossing.is_border_crossing = 1
                and
                border_crossing.id_doc_parent_operation is null
                and
                border_crossing.deleted = 0
                and
                border_crossing.id < new.id
                and
                border_crossing.__border_crossing_for_order = true;

            update public.order as orders set
                id_border_crossing = new.id,
                date_delivery = coalesce(
                    new.end_expected_date,
                    orders.date_delivery
                )
            where
                new.id_order = orders.id
                and
                (
                    orders.id_border_crossing is distinct from new.id
                    or
                    orders.date_delivery is distinct from coalesce(
                        new.end_expected_date,
                        orders.date_delivery
                    )
                );

        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_border_crossing_for_order_on_operation
after insert or update of deleted, end_expected_date, id_doc_parent_operation, id_order, is_border_crossing or delete
on operation.operation
for each row
execute procedure cache_border_crossing_for_order_on_operation();