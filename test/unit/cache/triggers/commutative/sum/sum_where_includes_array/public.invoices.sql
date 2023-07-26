create or replace function cache_invoices_for_orders_on_invoice()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_orders_ids bigint[];
declare not_changed_orders_ids bigint[];
declare deleted_orders_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if
            old.orders_ids is not null
            and
            old.deleted = 0
        then
            update orders set
                __invoices_json__ = __invoices_json__ - old.id::text,
                (
                    invoices_profit
                ) = (
                    select
                            sum(source_row.profit) as invoices_profit
                    from (
                        select
                                record.*
                        from jsonb_each(
    __invoices_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids @> ARRAY[orders.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                orders.id = any( old.orders_ids::bigint[] );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            cm_equal_arrays(new.orders_ids, old.orders_ids)
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        matched_old = coalesce(old.deleted = 0, false);
        matched_new = coalesce(new.deleted = 0, false);

        if
            not matched_old
            and
            not matched_new
        then
            return new;
        end if;

        if
            matched_old
            and
            not matched_new
        then
            inserted_orders_ids = null;
            not_changed_orders_ids = null;
            deleted_orders_ids = old.orders_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_orders_ids = new.orders_ids;
            not_changed_orders_ids = null;
            deleted_orders_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_orders_ids = cm_get_inserted_elements(old.orders_ids, new.orders_ids);
            not_changed_orders_ids = cm_get_not_changed_elements(old.orders_ids, new.orders_ids);
            deleted_orders_ids = cm_get_deleted_elements(old.orders_ids, new.orders_ids);
        end if;

        if not_changed_orders_ids is not null then
            update orders set
                __invoices_json__ = cm_merge_json(
            __invoices_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'orders_ids', new.orders_ids,'profit', new.profit
            ),
            TG_OP
        ),
                (
                    invoices_profit
                ) = (
                    select
                            sum(source_row.profit) as invoices_profit
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __invoices_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'orders_ids', new.orders_ids,'profit', new.profit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids @> ARRAY[orders.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                orders.id = any( not_changed_orders_ids::bigint[] );
        end if;

        if deleted_orders_ids is not null then
            update orders set
                __invoices_json__ = __invoices_json__ - old.id::text,
                (
                    invoices_profit
                ) = (
                    select
                            sum(source_row.profit) as invoices_profit
                    from (
                        select
                                record.*
                        from jsonb_each(
    __invoices_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids @> ARRAY[orders.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                orders.id = any( deleted_orders_ids::bigint[] );
        end if;

        if inserted_orders_ids is not null then
            update orders set
                __invoices_json__ = cm_merge_json(
            __invoices_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'orders_ids', new.orders_ids,'profit', new.profit
            ),
            TG_OP
        ),
                (
                    invoices_profit
                ) = (
                    select
                            sum(source_row.profit) as invoices_profit
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __invoices_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'orders_ids', new.orders_ids,'profit', new.profit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids @> ARRAY[orders.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                orders.id = any( inserted_orders_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.orders_ids is not null
            and
            new.deleted = 0
        then
            update orders set
                __invoices_json__ = cm_merge_json(
            __invoices_json__,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'orders_ids', new.orders_ids,'profit', new.profit
            ),
            TG_OP
        ),
                (
                    invoices_profit
                ) = (
                    select
                            sum(source_row.profit) as invoices_profit
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __invoices_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'orders_ids', new.orders_ids,'profit', new.profit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.invoice, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.orders_ids @> ARRAY[orders.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                orders.id = any( new.orders_ids::bigint[] );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_invoices_for_orders_on_invoice
after insert or update of deleted, orders_ids, profit or delete
on public.invoice
for each row
execute procedure cache_invoices_for_orders_on_invoice();