create or replace function cache_payments_for_invoice_on_payment_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.deleted = 0 then
            update invoice set
                __payments_json__ = __payments_json__ - old.id::text,
                (
                    payments_total
                ) = (
                    select
                            sum(source_row.total) as payments_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    __payments_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.payment_orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.deleted = 0
                        and
                        source_row.id = any(invoice.payments_ids)
                )
            where
                invoice.payments_ids && cm_build_array_for((null::public.invoice).payments_ids, old.id);
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
                __payments_json__ = cm_merge_json(
            __payments_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'total', new.total
            ),
            TG_OP
        ),
                (
                    payments_total
                ) = (
                    select
                            sum(source_row.total) as payments_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __payments_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'total', new.total
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.payment_orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.deleted = 0
                        and
                        source_row.id = any(invoice.payments_ids)
                )
            where
                invoice.payments_ids && cm_build_array_for((null::public.invoice).payments_ids, new.id);

            return new;
        end if;

        if old.deleted = 0 then
            update invoice set
                __payments_json__ = __payments_json__ - old.id::text,
                (
                    payments_total
                ) = (
                    select
                            sum(source_row.total) as payments_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    __payments_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.payment_orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.deleted = 0
                        and
                        source_row.id = any(invoice.payments_ids)
                )
            where
                invoice.payments_ids && cm_build_array_for((null::public.invoice).payments_ids, old.id);
        end if;

        if new.deleted = 0 then
            update invoice set
                __payments_json__ = cm_merge_json(
            __payments_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'total', new.total
            ),
            TG_OP
        ),
                (
                    payments_total
                ) = (
                    select
                            sum(source_row.total) as payments_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __payments_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'total', new.total
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.payment_orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.deleted = 0
                        and
                        source_row.id = any(invoice.payments_ids)
                )
            where
                invoice.payments_ids && cm_build_array_for((null::public.invoice).payments_ids, new.id);
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