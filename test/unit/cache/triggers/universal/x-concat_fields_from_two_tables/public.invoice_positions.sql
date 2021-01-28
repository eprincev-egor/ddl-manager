create or replace function cache_orders_for_invoice_on_invoice_positions()
returns trigger as $body$
declare old_unit_id_order text;
declare old_order_profit text;
declare new_unit_id_order text;
declare new_order_profit text;
begin

    if TG_OP = 'DELETE' then

        if
            old.id_invoice is not null
            and
            old.deleted = 0
        then
            if old.id_unit is not null then
                old_unit_id_order = (
                    select
                        coalesce(operation.unit.id_order, old.id_order)
                    from operation.unit
                    where
                        operation.unit.id = old.id_unit
                );
            end if;

            if old_unit_id_order is not null then
                old_order_profit = (
                    select
                        orders.profit
                    from orders
                    where
                        orders.id = old_unit_id_order
                );
            end if;

            update invoice set
                orders_profit = orders_profit - coalesce(old_order_profit, 0)
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
            new.id_unit is not distinct from old.id_unit
        then
            return new;
        end if;

        if old.id_unit is not null then
            old_unit_id_order = (
                select
                    operation.unit.id_order
                from operation.unit
                where
                    operation.unit.id = old.id_unit
            );
        end if;

        if old_unit_id_order is not null then
            old_order_profit = (
                select
                    orders.profit
                from orders
                where
                    orders.id = old_unit_id_order
            );
        end if;

        if new.id_unit is not distinct from old.id_unit then
            new_unit_id_order = old_unit_id_order;
        else
            if new.id_unit is not null then
                new_unit_id_order = (
                    select
                        operation.unit.id_order
                    from operation.unit
                    where
                        operation.unit.id = new.id_unit
                );
            end if;
        end if;

        if new.id_order is not distinct from old.id_order then
            new_order_profit = old_order_profit;
        else
            if new.id_order is not null then
                new_order_profit = (
                    select
                        orders.profit
                    from orders
                    where
                        orders.id = new_unit_id_order
                );
            end if;
        end if;

        if
            new.id_invoice is not distinct from old.id_invoice
            and
            new.deleted is not distinct from old.deleted
        then
            update invoice set
                orders_profit = orders_profit - coalesce(old_order_profit, 0) + coalesce(new_order_profit, 0)
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
                orders_profit = orders_profit - coalesce(old_order_profit, 0)
            where
                old.id_invoice = invoice.id;
        end if;

        if
            new.id_invoice is not null
            and
            new.deleted = 0
        then
            update invoice set
                orders_profit = orders_profit + coalesce(new_order_profit, 0)
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
            if new.id_unit is not null then
                new_unit_id_order = (
                    select
                        operation.unit.id_order
                    from operation.unit
                    where
                        operation.unit.id = new.id_unit
                );
            end if;

            if new_unit_id_order is not null then
                new_order_profit = (
                    select
                        orders.profit
                    from orders
                    where
                        orders.id = new_unit_id_order
                );
            end if;

            update invoice set
                orders_profit = orders_profit + coalesce(new_order_profit, 0)
            where
                new.id_invoice = invoice.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_orders_for_invoice_on_invoice_positions
after insert or update of deleted, id_invoice, id_unit or delete
on public.invoice_positions
for each row
execute procedure cache_orders_for_invoice_on_invoice_positions();