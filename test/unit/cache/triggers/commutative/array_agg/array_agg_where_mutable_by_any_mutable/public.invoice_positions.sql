create or replace function cache_invoices_for_list_gtd_on_invoice_positions()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_operation_unit is not null
            and
            old.deleted = 0
        then
            update list_gtd as gtd set
                __invoices_json__ = __invoices_json__ - old.id::text,
                (
                    by_unit_invoices_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_invoice) as by_unit_invoices_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __invoices_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_operation_unit = any (gtd.operation_units_ids)
                        and
                        source_row.deleted = 0
                )
            where
                gtd.operation_units_ids && ARRAY[ old.id_operation_unit ]::bigint[];
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_invoice is not distinct from old.id_invoice
            and
            new.id_operation_unit is not distinct from old.id_operation_unit
        then
            return new;
        end if;

        if
            new.id_operation_unit is not distinct from old.id_operation_unit
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_operation_unit is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update list_gtd as gtd set
                __invoices_json__ = cm_merge_json(
            __invoices_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_unit', new.id_operation_unit
            ),
            TG_OP
        ),
                (
                    by_unit_invoices_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_invoice) as by_unit_invoices_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __invoices_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_unit', new.id_operation_unit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_operation_unit = any (gtd.operation_units_ids)
                        and
                        source_row.deleted = 0
                )
            where
                gtd.operation_units_ids && ARRAY[ new.id_operation_unit ]::bigint[];

            return new;
        end if;

        if
            old.id_operation_unit is not null
            and
            old.deleted = 0
        then
            update list_gtd as gtd set
                __invoices_json__ = __invoices_json__ - old.id::text,
                (
                    by_unit_invoices_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_invoice) as by_unit_invoices_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __invoices_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_operation_unit = any (gtd.operation_units_ids)
                        and
                        source_row.deleted = 0
                )
            where
                gtd.operation_units_ids && ARRAY[ old.id_operation_unit ]::bigint[];
        end if;

        if
            new.id_operation_unit is not null
            and
            new.deleted = 0
        then
            update list_gtd as gtd set
                __invoices_json__ = cm_merge_json(
            __invoices_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_unit', new.id_operation_unit
            ),
            TG_OP
        ),
                (
                    by_unit_invoices_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_invoice) as by_unit_invoices_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __invoices_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_unit', new.id_operation_unit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_operation_unit = any (gtd.operation_units_ids)
                        and
                        source_row.deleted = 0
                )
            where
                gtd.operation_units_ids && ARRAY[ new.id_operation_unit ]::bigint[];
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_operation_unit is not null
            and
            new.deleted = 0
        then
            update list_gtd as gtd set
                __invoices_json__ = cm_merge_json(
            __invoices_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_unit', new.id_operation_unit
            ),
            TG_OP
        ),
                (
                    by_unit_invoices_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_invoice) as by_unit_invoices_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __invoices_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_unit', new.id_operation_unit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_operation_unit = any (gtd.operation_units_ids)
                        and
                        source_row.deleted = 0
                )
            where
                gtd.operation_units_ids && ARRAY[ new.id_operation_unit ]::bigint[];
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_invoices_for_list_gtd_on_invoice_positions
after insert or update of deleted, id_invoice, id_operation_unit or delete
on public.invoice_positions
for each row
execute procedure cache_invoices_for_list_gtd_on_invoice_positions();