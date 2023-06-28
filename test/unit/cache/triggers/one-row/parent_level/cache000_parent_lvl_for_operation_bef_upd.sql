create or replace function cache000_parent_lvl_for_operation_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if new.id_parent_operation is not distinct from old.id_parent_operation then
        return new;
    end if;


    select
            coalesce(parent_oper.lvl, 0) + 1 as lvl
    from operation.operation as parent_oper
    where
        parent_oper.id = new.id_parent_operation
    into new_totals;


    new.lvl = new_totals.lvl;


    return new;
end
$body$
language plpgsql;

create trigger cache000_parent_lvl_for_operation_bef_upd
before update of id_parent_operation
on operation.operation
for each row
execute procedure cache000_parent_lvl_for_operation_bef_upd();