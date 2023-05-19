create or replace function cache_parent_lvl_for_self_on_operation()
returns trigger as $body$
begin

    if new.id_parent_operation is not distinct from old.id_parent_operation then
        return new;
    end if;


    update operation.operation as child_oper set
        (
            lvl
        ) = (
            select
                coalesce(parent_oper.lvl, 0) + 1 as lvl

            from operation.operation as parent_oper

            where
                parent_oper.id = child_oper.id_parent_operation
        )
    where
        child_oper.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_parent_lvl_for_self_on_operation
after update of id_parent_operation
on operation.operation
for each row
execute procedure cache_parent_lvl_for_self_on_operation();