create or replace function cache_parent_row_for_operation_on_operation()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if
            old.id_order is not null
            or
            coalesce(old.lvl, 0) + 1 is not null
        then
            update operation.operation as child_oper set
                parent_id_order = null,
                lvl = coalesce(null, 0) + 1
            where
                old.id = child_oper.id_parent_operation
                and
                (
                    child_oper.parent_id_order is distinct from (null)
                    or
                    child_oper.lvl is distinct from (coalesce(null, 0) + 1)
                );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_order is not distinct from old.id_order
            and
            new.lvl is not distinct from old.lvl
        then
            return new;
        end if;

        update operation.operation as child_oper set
            parent_id_order = new.id_order,
            lvl = coalesce(new.lvl, 0) + 1
        where
            new.id = child_oper.id_parent_operation
            and
            (
                child_oper.parent_id_order is distinct from (new.id_order)
                or
                child_oper.lvl is distinct from (coalesce(new.lvl, 0) + 1)
            );

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.id_order is not null
            or
            coalesce(new.lvl, 0) + 1 is not null
        then
            update operation.operation as child_oper set
                parent_id_order = new.id_order,
                lvl = coalesce(new.lvl, 0) + 1
            where
                new.id = child_oper.id_parent_operation
                and
                (
                    child_oper.parent_id_order is distinct from (new.id_order)
                    or
                    child_oper.lvl is distinct from (coalesce(new.lvl, 0) + 1)
                );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_parent_row_for_operation_on_operation
after insert or update of id_order, id_parent_operation, lvl or delete
on operation.operation
for each row
execute procedure cache_parent_row_for_operation_on_operation();