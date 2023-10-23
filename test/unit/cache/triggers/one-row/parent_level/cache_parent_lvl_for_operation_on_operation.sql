create or replace function cache_parent_lvl_for_operation_on_operation()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if coalesce(old.lvl, 0) + 1 is not null then
            update operation.operation as child_oper set
                lvl = coalesce((null::integer), 0) + 1
            where
                old.id = child_oper.id_parent_operation
                and
                child_oper.lvl is distinct from (coalesce((null::integer), 0) + 1);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.lvl is not distinct from old.lvl then
            return new;
        end if;

        update operation.operation as child_oper set
            lvl = coalesce(new.lvl, 0) + 1
        where
            new.id = child_oper.id_parent_operation
            and
            child_oper.lvl is distinct from (coalesce(new.lvl, 0) + 1);

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if coalesce(new.lvl, 0) + 1 is not null then
            update operation.operation as child_oper set
                lvl = coalesce(new.lvl, 0) + 1
            where
                new.id = child_oper.id_parent_operation
                and
                child_oper.lvl is distinct from (coalesce(new.lvl, 0) + 1);
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_parent_lvl_for_operation_on_operation
after insert or update of id_parent_operation, lvl or delete
on operation.operation
for each row
execute procedure cache_parent_lvl_for_operation_on_operation();