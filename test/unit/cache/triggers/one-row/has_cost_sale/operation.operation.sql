create or replace function cache_has_cost_sale_for_fin_operation_on_operation()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if
            old.cost_sale is not null
            and
            old.deleted = 0 is not null
        then
            update operation.fin_operation as fin_op set
                is_cost_sale = null is not null
                and
                null = 0
            where
                old.id = fin_op.id_source_operation_sale
                and
                fin_op.is_cost_sale is distinct from (null is not null
                and
                null = 0);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.cost_sale is not distinct from old.cost_sale
            and
            new.deleted is not distinct from old.deleted
        then
            return new;
        end if;

        update operation.fin_operation as fin_op set
            is_cost_sale = new.cost_sale is not null
            and
            new.deleted = 0
        where
            new.id = fin_op.id_source_operation_sale
            and
            fin_op.is_cost_sale is distinct from (new.cost_sale is not null
            and
            new.deleted = 0);

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.cost_sale is not null
            and
            new.deleted = 0 is not null
        then
            update operation.fin_operation as fin_op set
                is_cost_sale = new.cost_sale is not null
                and
                new.deleted = 0
            where
                new.id = fin_op.id_source_operation_sale
                and
                fin_op.is_cost_sale is distinct from (new.cost_sale is not null
                and
                new.deleted = 0);
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_has_cost_sale_for_fin_operation_on_operation
after insert or update of cost_sale, deleted or delete
on operation.operation
for each row
execute procedure cache_has_cost_sale_for_fin_operation_on_operation();