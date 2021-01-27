create or replace function cache_payments_for_self_on_invoice()
returns trigger as $body$
begin
    if TG_OP = 'INSERT' then
        if new.payments_ids is null then
            return new;
        end if;
    end if;

    if TG_OP = 'UPDATE' then
        if cm_equal_arrays(new.payments_ids, old.payments_ids) then
            return new;
        end if;
    end if;


    update invoice set
        (
            payments_no_number,
            payments_no
        ) = (
            select
                array_agg(payment_orders.number) as payments_no_number,
                string_agg(distinct
        payment_orders.number,
        ', '
    ) as payments_no

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
after insert or update of payments_ids
on public.invoice
for each row
execute procedure cache_payments_for_self_on_invoice();