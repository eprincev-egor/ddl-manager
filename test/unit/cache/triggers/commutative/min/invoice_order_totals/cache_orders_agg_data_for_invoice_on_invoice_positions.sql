create or replace function cache_orders_agg_data_for_invoice_on_invoice_positions()
returns trigger as $body$
declare old_order_some_date text;
declare new_order_some_date text;
begin

    if TG_OP = 'DELETE' then

        if old.id_invoice is not null then
            if old.id_order is not null then
                old_order_some_date = (
                    select
                        public.order.some_date
                    from public.order
                    where
                        public.order.id = old.id_order
                );
            end if;

            update invoice set
                order_some_date_some_date = cm_array_remove_one_element(
                    order_some_date_some_date,
                    old_order_some_date
                ),
                order_some_date = (
                    select
                        min(item.some_date)

                    from unnest(
                        cm_array_remove_one_element(
                            order_some_date_some_date,
                            old_order_some_date
                        )
                    ) as item(some_date)
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

        if old.id_order is not null then
            old_order_some_date = (
                select
                    public.order.some_date
                from public.order
                where
                    public.order.id = old.id_order
            );
        end if;

        if new.id_order is not distinct from old.id_order then
            new_order_some_date = old_order_some_date;
        else
            if new.id_order is not null then
                new_order_some_date = (
                    select
                        public.order.some_date
                    from public.order
                    where
                        public.order.id = new.id_order
                );
            end if;
        end if;

        if new.id_invoice is not distinct from old.id_invoice then
            if new.id_invoice is null then
                return new;
            end if;

            update invoice set
                order_some_date_some_date = array_append(
                    cm_array_remove_one_element(
                        order_some_date_some_date,
                        old_order_some_date
                    ),
                    new_order_some_date
                ),
                order_some_date = (
                    select
                        min(item.some_date)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                order_some_date_some_date,
                                old_order_some_date
                            ),
                            new_order_some_date
                        )
                    ) as item(some_date)
                )
            where
                new.id_invoice = invoice.id
                and
                invoice.id_invoice_type = 4;

            return new;
        end if;

        if old.id_invoice is not null then
            update invoice set
                order_some_date_some_date = cm_array_remove_one_element(
                    order_some_date_some_date,
                    old_order_some_date
                ),
                order_some_date = (
                    select
                        min(item.some_date)

                    from unnest(
                        cm_array_remove_one_element(
                            order_some_date_some_date,
                            old_order_some_date
                        )
                    ) as item(some_date)
                )
            where
                old.id_invoice = invoice.id
                and
                invoice.id_invoice_type = 4;
        end if;

        if new.id_invoice is not null then
            update invoice set
                order_some_date_some_date = array_append(
                    order_some_date_some_date,
                    new_order_some_date
                ),
                order_some_date = least(
                    order_some_date,
                    new_order_some_date
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
            if new.id_order is not null then
                new_order_some_date = (
                    select
                        public.order.some_date
                    from public.order
                    where
                        public.order.id = new.id_order
                );
            end if;

            update invoice set
                order_some_date_some_date = array_append(
                    order_some_date_some_date,
                    new_order_some_date
                ),
                order_some_date = least(
                    order_some_date,
                    new_order_some_date
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