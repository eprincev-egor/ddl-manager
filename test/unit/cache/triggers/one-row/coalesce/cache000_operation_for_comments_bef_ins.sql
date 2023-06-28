create or replace function cache000_operation_for_comments_bef_ins()
returns trigger as $body$
declare new_totals record;
begin


    if not coalesce(new.query_name = 'OPERATION', false) then
        return new;
    end if;


    select
            coalesce(
                operation.operation.doc_parent_id_order,
                operation.operation.id_order
                        ) as operation_id_order,
            operation.operation.id_operation_type as operation_type_id
    from operation.operation
    where
        operation.operation.id = new.row_id
        and
        operation.operation.deleted = 0
        and
        new.query_name = 'OPERATION'
    into new_totals;


    new.operation_id_order = new_totals.operation_id_order;
    new.operation_type_id = new_totals.operation_type_id;


    return new;
end
$body$
language plpgsql;

create trigger cache000_operation_for_comments_bef_ins
before insert
on public.comments
for each row
execute procedure cache000_operation_for_comments_bef_ins();