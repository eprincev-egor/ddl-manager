create or replace function cache_orders_agg_data_for_invoice_on_invoice_positions()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_invoice is not null then
            update invoice set
                __orders_agg_data_json__ = __orders_agg_data_json__ - old.id::text,
                (
                    order_some_date
                ) = (
                    select
                            min(orders.some_date) as order_some_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    __orders_agg_data_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join public.order as orders on
                        orders.id = source_row.id_order
                    where
                        invoice.id_invoice_type = 4
                        and
                        source_row.id_invoice = invoice.id
                )
            where
                old.id_invoice = invoice.id
                and
                invoice.id_invoice_type = 4;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_invoice is not distinct from old.id_invoice
            and
            new.id_order is not distinct from old.id_order
        then
            return new;
        end if;

        if new.id_invoice is not distinct from old.id_invoice then
            if new.id_invoice is null then
                return new;
            end if;

            update invoice set
                __orders_agg_data_json__ = cm_merge_json(
            __orders_agg_data_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_invoice', new.id_invoice,'id_order', new.id_order
            ),
            TG_OP
        ),
                (
                    order_some_date
                ) = (
                    select
                            min(orders.some_date) as order_some_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __orders_agg_data_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_invoice', new.id_invoice,'id_order', new.id_order
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join public.order as orders on
                        orders.id = source_row.id_order
                    where
                        invoice.id_invoice_type = 4
                        and
                        source_row.id_invoice = invoice.id
                )
            where
                new.id_invoice = invoice.id
                and
                invoice.id_invoice_type = 4;

            return new;
        end if;

        if old.id_invoice is not null then
            update invoice set
                __orders_agg_data_json__ = __orders_agg_data_json__ - old.id::text,
                (
                    order_some_date
                ) = (
                    select
                            min(orders.some_date) as order_some_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    __orders_agg_data_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join public.order as orders on
                        orders.id = source_row.id_order
                    where
                        invoice.id_invoice_type = 4
                        and
                        source_row.id_invoice = invoice.id
                )
            where
                old.id_invoice = invoice.id
                and
                invoice.id_invoice_type = 4;
        end if;

        if new.id_invoice is not null then
            update invoice set
                __orders_agg_data_json__ = cm_merge_json(
            __orders_agg_data_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_invoice', new.id_invoice,'id_order', new.id_order
            ),
            TG_OP
        ),
                (
                    order_some_date
                ) = (
                    select
                            min(orders.some_date) as order_some_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __orders_agg_data_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_invoice', new.id_invoice,'id_order', new.id_order
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join public.order as orders on
                        orders.id = source_row.id_order
                    where
                        invoice.id_invoice_type = 4
                        and
                        source_row.id_invoice = invoice.id
                )
            where
                new.id_invoice = invoice.id
                and
                invoice.id_invoice_type = 4;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_invoice is not null then
            update invoice set
                __orders_agg_data_json__ = cm_merge_json(
            __orders_agg_data_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_invoice', new.id_invoice,'id_order', new.id_order
            ),
            TG_OP
        ),
                (
                    order_some_date
                ) = (
                    select
                            min(orders.some_date) as order_some_date
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __orders_agg_data_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_invoice', new.id_invoice,'id_order', new.id_order
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice_positions, json_entry.value) as record on
                            true
                    ) as source_row

                    left join public.order as orders on
                        orders.id = source_row.id_order
                    where
                        invoice.id_invoice_type = 4
                        and
                        source_row.id_invoice = invoice.id
                )
            where
                new.id_invoice = invoice.id
                and
                invoice.id_invoice_type = 4;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_orders_agg_data_for_invoice_on_invoice_positions
after insert or update of id_invoice, id_order or delete
on public.invoice_positions
for each row
execute procedure cache_orders_agg_data_for_invoice_on_invoice_positions();