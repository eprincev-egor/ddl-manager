create or replace function cache000_has_cost_sale_for_fin_operation_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
            operation.operation.cost_sale is not null
            and
            operation.operation.deleted = 0 as is_cost_sale
    from operation.operation
    where
        operation.operation.id = new.id_source_operation_sale
    into new_totals;


    new.is_cost_sale = new_totals.is_cost_sale;


    return new;
end
$body$
language plpgsql;

create trigger cache000_has_cost_sale_for_fin_operation_bef_ins
before insert
on operation.fin_operation
for each row
execute procedure cache000_has_cost_sale_for_fin_operation_bef_ins();