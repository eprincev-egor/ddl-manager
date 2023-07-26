create or replace function cache000_payments_for_invoice_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
            sum(payment_orders.total) as payments_total,
            ('{' || string_agg(
                                        '"' || public.payment_orders.id::text || '":' || jsonb_build_object(
                            'deleted', public.payment_orders.deleted,'id', public.payment_orders.id,'total', public.payment_orders.total
                        )::text,
                                        ','
                                    ) || '}')
            ::
            jsonb as __payments_json__
    from payment_orders
    where
        payment_orders.deleted = 0
        and
        payment_orders.id = any (new.payments_ids)
    into new_totals;


    new.payments_total = new_totals.payments_total;
    new.__payments_json__ = new_totals.__payments_json__;


    return new;
end
$body$
language plpgsql;

create trigger cache000_payments_for_invoice_bef_ins
before insert
on public.invoice
for each row
execute procedure cache000_payments_for_invoice_bef_ins();