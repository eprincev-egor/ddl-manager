create or replace function cache000_parent_lvl_for_operation_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



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

create trigger cache000_parent_lvl_for_operation_bef_ins
before insert
on operation.operation
for each row
execute procedure cache000_parent_lvl_for_operation_bef_ins();