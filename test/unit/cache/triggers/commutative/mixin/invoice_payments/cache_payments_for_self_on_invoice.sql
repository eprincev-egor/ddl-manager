create or replace function cache_payments_for_self_on_invoice()
returns trigger as $body$
begin

    if cm_equal_arrays(new.payments_ids, old.payments_ids) then
        return new;
    end if;


    update invoice set
        (
            payments_total
        ) = (
            select
                sum(payment_orders.total) as payments_total

            from payment_orders

            where
                payment_orders.deleted = 0
    and
    payment_orders.id = any (invoice.payments_ids)
        )
    where
        public.invoice.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_payments_for_self_on_invoice
after update of payments_ids
on public.invoice
for each row
execute procedure cache_payments_for_self_on_invoice();