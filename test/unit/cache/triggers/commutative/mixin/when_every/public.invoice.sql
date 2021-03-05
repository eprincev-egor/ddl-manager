create or replace function cache_test_for_order_on_invoice()
returns trigger as $body$
declare inserted_orders_ids bigint[];
declare deleted_orders_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if
            old.orders_ids is not null
            and
            old.id_invoice_type = 2
            and
            old.deleted = 0
        then
            update public.order set
                all_invoices_has_payment_every_payment_date = cm_array_remove_one_element(
                    all_invoices_has_payment_every_payment_date,
                    old.payment_date
                ),
                all_invoices_has_payment_every = (
                    select
                        every(
                            item.payment_date is not null
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            all_invoices_has_payment_every_payment_date,
                            old.payment_date
                        )
                    ) as item(payment_date)
                ),
                all_invoices_has_payment = case
                    when
                        ((
                            select
                                every(
                                    item.payment_date is not null
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    all_invoices_has_payment_every_payment_date,
                                    old.payment_date
                                )
                            ) as item(payment_date)
                        ))
                    then
                        1
                    else
                        0
                end
            where
                public.order.id = any( old.orders_ids::bigint[] );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_invoice_type is not distinct from old.id_invoice_type
            and
            cm_equal_arrays(new.orders_ids, old.orders_ids)
            and
            new.payment_date is not distinct from old.payment_date
        then
            return new;
        end if;

        inserted_orders_ids = cm_get_inserted_elements(old.orders_ids, new.orders_ids);
        deleted_orders_ids = cm_get_deleted_elements(old.orders_ids, new.orders_ids);

        if
            cm_equal_arrays(new.orders_ids, old.orders_ids)
            and
            new.id_invoice_type is not distinct from old.id_invoice_type
            and
            new.deleted is not distinct from old.deleted
        then
            if
                not coalesce(new.id_invoice_type = 2, false)
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update public.order set
                all_invoices_has_payment_every_payment_date = array_append(
                    cm_array_remove_one_element(
                        all_invoices_has_payment_every_payment_date,
                        old.payment_date
                    ),
                    new.payment_date
                ),
                all_invoices_has_payment_every = (
                    select
                        every(
                            item.payment_date is not null
                        )

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                all_invoices_has_payment_every_payment_date,
                                old.payment_date
                            ),
                            new.payment_date
                        )
                    ) as item(payment_date)
                ),
                all_invoices_has_payment = case
                    when
                        ((
                            select
                                every(
                                    item.payment_date is not null
                                )

                            from unnest(
                                array_append(
                                    cm_array_remove_one_element(
                                        all_invoices_has_payment_every_payment_date,
                                        old.payment_date
                                    ),
                                    new.payment_date
                                )
                            ) as item(payment_date)
                        ))
                    then
                        1
                    else
                        0
                end
            where
                public.order.id = any( new.orders_ids::bigint[] );

            return new;
        end if;

        if
            deleted_orders_ids is not null
            and
            old.id_invoice_type = 2
            and
            old.deleted = 0
        then
            update public.order set
                all_invoices_has_payment_every_payment_date = cm_array_remove_one_element(
                    all_invoices_has_payment_every_payment_date,
                    old.payment_date
                ),
                all_invoices_has_payment_every = (
                    select
                        every(
                            item.payment_date is not null
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            all_invoices_has_payment_every_payment_date,
                            old.payment_date
                        )
                    ) as item(payment_date)
                ),
                all_invoices_has_payment = case
                    when
                        ((
                            select
                                every(
                                    item.payment_date is not null
                                )

                            from unnest(
                                cm_array_remove_one_element(
                                    all_invoices_has_payment_every_payment_date,
                                    old.payment_date
                                )
                            ) as item(payment_date)
                        ))
                    then
                        1
                    else
                        0
                end
            where
                public.order.id = any( deleted_orders_ids::bigint[] );
        end if;

        if
            inserted_orders_ids is not null
            and
            new.id_invoice_type = 2
            and
            new.deleted = 0
        then
            update public.order set
                all_invoices_has_payment_every_payment_date = array_append(
                    all_invoices_has_payment_every_payment_date,
                    new.payment_date
                ),
                all_invoices_has_payment_every = all_invoices_has_payment_every
                and
                new.payment_date is not null,
                all_invoices_has_payment = case
                    when
                        (all_invoices_has_payment_every
                        and
                        new.payment_date is not null)
                    then
                        1
                    else
                        0
                end
            where
                public.order.id = any( inserted_orders_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.orders_ids is not null
            and
            new.id_invoice_type = 2
            and
            new.deleted = 0
        then
            update public.order set
                all_invoices_has_payment_every_payment_date = array_append(
                    all_invoices_has_payment_every_payment_date,
                    new.payment_date
                ),
                all_invoices_has_payment_every = all_invoices_has_payment_every
                and
                new.payment_date is not null,
                all_invoices_has_payment = case
                    when
                        (all_invoices_has_payment_every
                        and
                        new.payment_date is not null)
                    then
                        1
                    else
                        0
                end
            where
                public.order.id = any( new.orders_ids::bigint[] );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_test_for_order_on_invoice
after insert or update of deleted, id_invoice_type, orders_ids, payment_date or delete
on public.invoice
for each row
execute procedure cache_test_for_order_on_invoice();