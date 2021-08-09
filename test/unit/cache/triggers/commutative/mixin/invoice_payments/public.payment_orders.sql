create or replace function cache_payments_for_invoice_on_payment_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.deleted = 0 then
            update invoice set
                payments_total = coalesce(payments_total, 0) - coalesce(old.total, 0)
            where
                invoice.payments_ids && ARRAY[ old.id ]::int8[];
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.total is not distinct from old.total
        then
            return new;
        end if;

        if new.deleted is not distinct from old.deleted then
            if not coalesce(new.deleted = 0, false) then
                return new;
            end if;

            update invoice set
                payments_total = coalesce(payments_total, 0) - coalesce(old.total, 0) + coalesce(new.total, 0)
            where
                invoice.payments_ids && ARRAY[ new.id ]::int8[];

            return new;
        end if;

        if old.deleted = 0 then
            update invoice set
                payments_total = coalesce(payments_total, 0) - coalesce(old.total, 0)
            where
                invoice.payments_ids && ARRAY[ old.id ]::int8[];
        end if;

        if new.deleted = 0 then
            update invoice set
                payments_total = coalesce(payments_total, 0) + coalesce(new.total, 0)
            where
                invoice.payments_ids && ARRAY[ new.id ]::int8[];
        end if;

        return new;
    end if;
end
$body$
language plpgsql;

create trigger cache_payments_for_invoice_on_payment_orders
after update of deleted, total or delete
on public.payment_orders
for each row
execute procedure cache_payments_for_invoice_on_payment_orders();