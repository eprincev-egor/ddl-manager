create or replace function cache_balance_for_invoice_on_invoice_positions()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_invoice is not null
            and
            old.deleted = 0
        then
            update invoice set
                __balance_json__ = __balance_json__ - old.id::text,
                (
                    balance
                ) = (
                    select
                            invoice.invoice_summ -                             sum(
                                round(
                                    source_row.total_sum_with_vat_in_curs :: numeric,
                                    - 2
                                    )
                                                        ) as balance
                    from (
                        select
                                record.*
                        from jsonb_each(
    __balance_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_invoice = invoice.id
                        and
                        source_row.deleted = 0
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
            new.total_sum_with_vat_in_curs is not distinct from old.total_sum_with_vat_in_curs
        then
            return new;
        end if;

        if
            new.id_invoice is not distinct from old.id_invoice
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_invoice is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update invoice set
                __balance_json__ = cm_merge_json(
            __balance_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'total_sum_with_vat_in_curs', new.total_sum_with_vat_in_curs
        ),
            TG_OP
        ),
                (
                    balance
                ) = (
                    select
                            invoice.invoice_summ -                             sum(
                                round(
                                    source_row.total_sum_with_vat_in_curs :: numeric,
                                    - 2
                                    )
                                                        ) as balance
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __balance_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'total_sum_with_vat_in_curs', new.total_sum_with_vat_in_curs
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_invoice = invoice.id
                        and
                        source_row.deleted = 0
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
                __balance_json__ = __balance_json__ - old.id::text,
                (
                    balance
                ) = (
                    select
                            invoice.invoice_summ -                             sum(
                                round(
                                    source_row.total_sum_with_vat_in_curs :: numeric,
                                    - 2
                                    )
                                                        ) as balance
                    from (
                        select
                                record.*
                        from jsonb_each(
    __balance_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_invoice = invoice.id
                        and
                        source_row.deleted = 0
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
                __balance_json__ = cm_merge_json(
            __balance_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'total_sum_with_vat_in_curs', new.total_sum_with_vat_in_curs
        ),
            TG_OP
        ),
                (
                    balance
                ) = (
                    select
                            invoice.invoice_summ -                             sum(
                                round(
                                    source_row.total_sum_with_vat_in_curs :: numeric,
                                    - 2
                                    )
                                                        ) as balance
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __balance_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'total_sum_with_vat_in_curs', new.total_sum_with_vat_in_curs
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_invoice = invoice.id
                        and
                        source_row.deleted = 0
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
            update invoice set
                __balance_json__ = cm_merge_json(
            __balance_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'total_sum_with_vat_in_curs', new.total_sum_with_vat_in_curs
        ),
            TG_OP
        ),
                (
                    balance
                ) = (
                    select
                            invoice.invoice_summ -                             sum(
                                round(
                                    source_row.total_sum_with_vat_in_curs :: numeric,
                                    - 2
                                    )
                                                        ) as balance
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __balance_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'total_sum_with_vat_in_curs', new.total_sum_with_vat_in_curs
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_invoice = invoice.id
                        and
                        source_row.deleted = 0
                )
            where
                new.id_invoice = invoice.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_balance_for_invoice_on_invoice_positions
after insert or update of deleted, id_invoice, total_sum_with_vat_in_curs or delete
on public.invoice_positions
for each row
execute procedure cache_balance_for_invoice_on_invoice_positions();