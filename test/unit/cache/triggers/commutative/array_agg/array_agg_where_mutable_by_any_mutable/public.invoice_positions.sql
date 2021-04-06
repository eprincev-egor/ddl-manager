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
                by_unit_invoices_ids_id_invoice = cm_array_remove_one_element(
                    by_unit_invoices_ids_id_invoice,
                    old.id_invoice
                ),
                by_unit_invoices_ids = (
                    select
                        array_agg(distinct item.id_invoice)

                    from unnest(
                        cm_array_remove_one_element(
                            by_unit_invoices_ids_id_invoice,
                            old.id_invoice
                        )
                    ) as item(id_invoice)
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
                by_unit_invoices_ids_id_invoice = array_append(
                    cm_array_remove_one_element(
                        by_unit_invoices_ids_id_invoice,
                        old.id_invoice
                    ),
                    new.id_invoice
                ),
                by_unit_invoices_ids = (
                    select
                        array_agg(distinct item.id_invoice)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                by_unit_invoices_ids_id_invoice,
                                old.id_invoice
                            ),
                            new.id_invoice
                        )
                    ) as item(id_invoice)
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
                by_unit_invoices_ids_id_invoice = cm_array_remove_one_element(
                    by_unit_invoices_ids_id_invoice,
                    old.id_invoice
                ),
                by_unit_invoices_ids = (
                    select
                        array_agg(distinct item.id_invoice)

                    from unnest(
                        cm_array_remove_one_element(
                            by_unit_invoices_ids_id_invoice,
                            old.id_invoice
                        )
                    ) as item(id_invoice)
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
                by_unit_invoices_ids_id_invoice = array_append(
                    by_unit_invoices_ids_id_invoice,
                    new.id_invoice
                ),
                by_unit_invoices_ids = case
                    when
                        array_position(
                            by_unit_invoices_ids,
                            new.id_invoice
                        )
                        is null
                    then
                        array_append(
                            by_unit_invoices_ids,
                            new.id_invoice
                        )
                    else
                        by_unit_invoices_ids
                end
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
                by_unit_invoices_ids_id_invoice = array_append(
                    by_unit_invoices_ids_id_invoice,
                    new.id_invoice
                ),
                by_unit_invoices_ids = case
                    when
                        array_position(
                            by_unit_invoices_ids,
                            new.id_invoice
                        )
                        is null
                    then
                        array_append(
                            by_unit_invoices_ids,
                            new.id_invoice
                        )
                    else
                        by_unit_invoices_ids
                end
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