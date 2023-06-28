create or replace function cache_pos_rate_for_invoice_on_invoice_positions()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_invoice is not null
            and
            old.deleted = 0
        then
            update invoice set
                __pos_rate_json__ = __pos_rate_json__ - old.id::text,
                (
                    total_base_cost
                ) = (
                    select
                            sum(rate_category.base_cost) as total_base_cost
                    from (
                        select
                                record.*
                        from jsonb_each(
    __pos_rate_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join operation.rate_expense_type as rate_type on
                        rate_type.id = source_row.id_operation_rate_expense_type

                    left join operation.rate_expense_category as rate_category on
                        rate_category.id = rate_type.id_rate_expense_category
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
            new.id_operation_rate_expense_type is not distinct from old.id_operation_rate_expense_type
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
                __pos_rate_json__ = cm_merge_json(
            __pos_rate_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_rate_expense_type', new.id_operation_rate_expense_type
        ),
            TG_OP
        ),
                (
                    total_base_cost
                ) = (
                    select
                            sum(rate_category.base_cost) as total_base_cost
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __pos_rate_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_rate_expense_type', new.id_operation_rate_expense_type
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join operation.rate_expense_type as rate_type on
                        rate_type.id = source_row.id_operation_rate_expense_type

                    left join operation.rate_expense_category as rate_category on
                        rate_category.id = rate_type.id_rate_expense_category
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
                __pos_rate_json__ = __pos_rate_json__ - old.id::text,
                (
                    total_base_cost
                ) = (
                    select
                            sum(rate_category.base_cost) as total_base_cost
                    from (
                        select
                                record.*
                        from jsonb_each(
    __pos_rate_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join operation.rate_expense_type as rate_type on
                        rate_type.id = source_row.id_operation_rate_expense_type

                    left join operation.rate_expense_category as rate_category on
                        rate_category.id = rate_type.id_rate_expense_category
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
                __pos_rate_json__ = cm_merge_json(
            __pos_rate_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_rate_expense_type', new.id_operation_rate_expense_type
        ),
            TG_OP
        ),
                (
                    total_base_cost
                ) = (
                    select
                            sum(rate_category.base_cost) as total_base_cost
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __pos_rate_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_rate_expense_type', new.id_operation_rate_expense_type
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join operation.rate_expense_type as rate_type on
                        rate_type.id = source_row.id_operation_rate_expense_type

                    left join operation.rate_expense_category as rate_category on
                        rate_category.id = rate_type.id_rate_expense_category
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
                __pos_rate_json__ = cm_merge_json(
            __pos_rate_json__,
            null::jsonb,
            jsonb_build_object(
            'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_rate_expense_type', new.id_operation_rate_expense_type
        ),
            TG_OP
        ),
                (
                    total_base_cost
                ) = (
                    select
                            sum(rate_category.base_cost) as total_base_cost
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __pos_rate_json__,
                null::jsonb,
                jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_invoice', new.id_invoice,'id_operation_rate_expense_type', new.id_operation_rate_expense_type
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join operation.rate_expense_type as rate_type on
                        rate_type.id = source_row.id_operation_rate_expense_type

                    left join operation.rate_expense_category as rate_category on
                        rate_category.id = rate_type.id_rate_expense_category
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

create trigger cache_pos_rate_for_invoice_on_invoice_positions
after insert or update of deleted, id_invoice, id_operation_rate_expense_type or delete
on public.invoice_positions
for each row
execute procedure cache_pos_rate_for_invoice_on_invoice_positions();