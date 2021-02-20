create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                general_orders_ids = case
                    when
                        old.id_order_type in (1, 2, 3)
                    then
                        cm_array_remove_one_element(
                            general_orders_ids,
                            old.id
                        )
                    else
                        general_orders_ids
                end,
                all_orders_ids = cm_array_remove_one_element(all_orders_ids, old.id)
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.id_order_type is not distinct from old.id_order_type
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                general_orders_ids = case
                    when
                        new.id_order_type in (1, 2, 3)
                        and
                        not coalesce(old.id_order_type in (1, 2, 3), false)
                    then
                        array_append(
                            general_orders_ids,
                            new.id
                        )
                    when
                        not coalesce(new.id_order_type in (1, 2, 3), false)
                        and
                        old.id_order_type in (1, 2, 3)
                    then
                        cm_array_remove_one_element(
                            general_orders_ids,
                            old.id
                        )
                    else
                        general_orders_ids
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                general_orders_ids = case
                    when
                        old.id_order_type in (1, 2, 3)
                    then
                        cm_array_remove_one_element(
                            general_orders_ids,
                            old.id
                        )
                    else
                        general_orders_ids
                end,
                all_orders_ids = cm_array_remove_one_element(all_orders_ids, old.id)
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                general_orders_ids = case
                    when
                        new.id_order_type in (1, 2, 3)
                    then
                        array_append(
                            general_orders_ids,
                            new.id
                        )
                    else
                        general_orders_ids
                end,
                all_orders_ids = array_append(all_orders_ids, new.id)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                general_orders_ids = case
                    when
                        new.id_order_type in (1, 2, 3)
                    then
                        array_append(
                            general_orders_ids,
                            new.id
                        )
                    else
                        general_orders_ids
                end,
                all_orders_ids = array_append(all_orders_ids, new.id)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, id_order_type or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();