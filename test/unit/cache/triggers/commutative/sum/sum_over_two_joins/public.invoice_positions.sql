create or replace function cache_pos_rate_for_invoice_on_invoice_positions()
returns trigger as $body$
declare old_operation_rate_expense_type_id_rate_expense_category text;
declare old_rate_expense_category_base_cost text;
declare new_operation_rate_expense_type_id_rate_expense_category text;
declare new_rate_expense_category_base_cost text;
begin

    if TG_OP = 'DELETE' then

        if
            old.id_invoice is not null
            and
            old.deleted = 0
        then
            if old.id_operation_rate_expense_type is not null then
                old_operation_rate_expense_type_id_rate_expense_category = (
                    select
                        operation.rate_expense_type.id_rate_expense_category
                    from operation.rate_expense_type
                    where
                        operation.rate_expense_type.id = old.id_operation_rate_expense_type
                );
            end if;

            if old_operation_rate_expense_type_id_rate_expense_category is not null then
                old_rate_expense_category_base_cost = (
                    select
                        operation.rate_expense_category.base_cost
                    from operation.rate_expense_category
                    where
                        operation.rate_expense_category.id = old_operation_rate_expense_type_id_rate_expense_category
                );
            end if;

            update invoice set
                total_base_cost = total_base_cost - coalesce(
                    old_rate_expense_category_base_cost,
                    0
                )
            where
                old.id_invoice = invoice.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_invoice is not distinct from old.id_invoice
            and
            new.id_operation_rate_expense_type is not distinct from old.id_operation_rate_expense_type
        then
            return new;
        end if;

        if old.id_operation_rate_expense_type is not null then
            old_operation_rate_expense_type_id_rate_expense_category = (
                select
                    operation.rate_expense_type.id_rate_expense_category
                from operation.rate_expense_type
                where
                    operation.rate_expense_type.id = old.id_operation_rate_expense_type
            );
        end if;

        if old_operation_rate_expense_type_id_rate_expense_category is not null then
            old_rate_expense_category_base_cost = (
                select
                    operation.rate_expense_category.base_cost
                from operation.rate_expense_category
                where
                    operation.rate_expense_category.id = old_operation_rate_expense_type_id_rate_expense_category
            );
        end if;

        if new.id_operation_rate_expense_type is not distinct from old.id_operation_rate_expense_type then
            new_operation_rate_expense_type_id_rate_expense_category = old_operation_rate_expense_type_id_rate_expense_category;
        else
            if new.id_operation_rate_expense_type is not null then
                new_operation_rate_expense_type_id_rate_expense_category = (
                    select
                        operation.rate_expense_type.id_rate_expense_category
                    from operation.rate_expense_type
                    where
                        operation.rate_expense_type.id = new.id_operation_rate_expense_type
                );
            end if;
        end if;

        if new_operation_rate_expense_type_id_rate_expense_category is not distinct from old_operation_rate_expense_type_id_rate_expense_category then
            new_rate_expense_category_base_cost = old_rate_expense_category_base_cost;
        else
            if new_operation_rate_expense_type_id_rate_expense_category is not null then
                new_rate_expense_category_base_cost = (
                    select
                        operation.rate_expense_category.base_cost
                    from operation.rate_expense_category
                    where
                        operation.rate_expense_category.id = new_operation_rate_expense_type_id_rate_expense_category
                );
            end if;
        end if;

        if
            new.id_invoice is not distinct from old.id_invoice
            and
            new.deleted is not distinct from old.deleted
        then
            if not coalesce(new.deleted = 0, false) then
                return new;
            end if;

            update invoice set
                total_base_cost = total_base_cost - coalesce(
                    old_rate_expense_category_base_cost,
                    0
                ) + coalesce(
                    new_rate_expense_category_base_cost,
                    0
                )
            where
                new.id_invoice = invoice.id;

            return new;
        end if;

        if
            old.id_invoice is not null
            and
            old.deleted = 0
        then
            update invoice set
                total_base_cost = total_base_cost - coalesce(
                    old_rate_expense_category_base_cost,
                    0
                )
            where
                old.id_invoice = invoice.id;
        end if;

        if
            new.id_invoice is not null
            and
            new.deleted = 0
        then
            update invoice set
                total_base_cost = total_base_cost + coalesce(
                    new_rate_expense_category_base_cost,
                    0
                )
            where
                new.id_invoice = invoice.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_invoice is not null
            and
            new.deleted = 0
        then
            if new.id_operation_rate_expense_type is not null then
                new_operation_rate_expense_type_id_rate_expense_category = (
                    select
                        operation.rate_expense_type.id_rate_expense_category
                    from operation.rate_expense_type
                    where
                        operation.rate_expense_type.id = new.id_operation_rate_expense_type
                );
            end if;

            if new_operation_rate_expense_type_id_rate_expense_category is not null then
                new_rate_expense_category_base_cost = (
                    select
                        operation.rate_expense_category.base_cost
                    from operation.rate_expense_category
                    where
                        operation.rate_expense_category.id = new_operation_rate_expense_type_id_rate_expense_category
                );
            end if;

            update invoice set
                total_base_cost = total_base_cost + coalesce(
                    new_rate_expense_category_base_cost,
                    0
                )
            where
                new.id_invoice = invoice.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_pos_rate_for_invoice_on_invoice_positions
after insert or update of deleted, id_invoice, id_operation_rate_expense_type or delete
on public.invoice_positions
for each row
execute procedure cache_pos_rate_for_invoice_on_invoice_positions();