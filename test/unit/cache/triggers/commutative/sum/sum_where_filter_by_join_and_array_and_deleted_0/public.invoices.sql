create or replace function cache_invoices_for_orders_on_invoice()
returns trigger as $body$
declare old_invoice_type_code text;
declare new_invoice_type_code text;
declare matched_old boolean;
declare matched_new boolean;
declare inserted_orders_ids bigint[];
declare not_changed_orders_ids bigint[];
declare deleted_orders_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if
            old.orders_ids is not null
            and
            old.deleted = 0
        then
            if old.id_invoice_type is not null then
                old_invoice_type_code = (
                    select
                        invoice_type.code
                    from invoice_type
                    where
                        invoice_type.id = old.id_invoice_type
                );
            end if;

            if old_invoice_type_code in ('incoming', 'outgoung') then
                update orders set
                    invoices_profit = coalesce(invoices_profit, 0) - coalesce(old.profit, 0)
                where
                    orders.id = any( old.orders_ids::bigint[] );
            end if;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_invoice_type is not distinct from old.id_invoice_type
            and
            cm_equal_arrays(new.orders_ids, old.orders_ids)
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if old.id_invoice_type is not null then
            old_invoice_type_code = (
                select
                    invoice_type.code
                from invoice_type
                where
                    invoice_type.id = old.id_invoice_type
            );
        end if;

        if new.id_invoice_type is not distinct from old.id_invoice_type then
            new_invoice_type_code = old_invoice_type_code;
        else
            if new.id_invoice_type is not null then
                new_invoice_type_code = (
                    select
                        invoice_type.code
                    from invoice_type
                    where
                        invoice_type.id = new.id_invoice_type
                );
            end if;
        end if;

        matched_old = coalesce(old.deleted = 0
            and
            old_invoice_type_code in ('incoming', 'outgoung'), false);
        matched_new = coalesce(new.deleted = 0
            and
            new_invoice_type_code in ('incoming', 'outgoung'), false);

        if
            not matched_old
            and
            not matched_new
        then
            return new;
        end if;

        if
            matched_old
            and
            not matched_new
        then
            inserted_orders_ids = null;
            not_changed_orders_ids = null;
            deleted_orders_ids = old.orders_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_orders_ids = new.orders_ids;
            not_changed_orders_ids = null;
            deleted_orders_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_orders_ids = cm_get_inserted_elements(old.orders_ids, new.orders_ids);
            not_changed_orders_ids = cm_get_not_changed_elements(old.orders_ids, new.orders_ids);
            deleted_orders_ids = cm_get_deleted_elements(old.orders_ids, new.orders_ids);
        end if;

        if not_changed_orders_ids is not null then
            update orders set
                invoices_profit = coalesce(invoices_profit, 0) - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                orders.id = any( not_changed_orders_ids::bigint[] );
        end if;

        if deleted_orders_ids is not null then
            update orders set
                invoices_profit = coalesce(invoices_profit, 0) - coalesce(old.profit, 0)
            where
                orders.id = any( deleted_orders_ids::bigint[] );
        end if;

        if inserted_orders_ids is not null then
            update orders set
                invoices_profit = coalesce(invoices_profit, 0) + coalesce(new.profit, 0)
            where
                orders.id = any( inserted_orders_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.orders_ids is not null
            and
            new.deleted = 0
        then
            if new.id_invoice_type is not null then
                new_invoice_type_code = (
                    select
                        invoice_type.code
                    from invoice_type
                    where
                        invoice_type.id = new.id_invoice_type
                );
            end if;

            if new_invoice_type_code in ('incoming', 'outgoung') then
                update orders set
                    invoices_profit = coalesce(invoices_profit, 0) + coalesce(new.profit, 0)
                where
                    orders.id = any( new.orders_ids::bigint[] );
            end if;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_invoices_for_orders_on_invoice
after insert or update of deleted, id_invoice_type, orders_ids, profit or delete
on public.invoice
for each row
execute procedure cache_invoices_for_orders_on_invoice();