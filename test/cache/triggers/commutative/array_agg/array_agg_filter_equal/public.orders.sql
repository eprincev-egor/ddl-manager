create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            (
                old.id_order_type = 1
                or
                old.id_order_type = 2
            )
        then
            update companies set
                fcl_orders_ids = case
                    when
                        old.id_order_type = 1
                    then
                        cm_array_remove_one_element(fcl_orders_ids, old.id)
                    else
                        fcl_orders_ids
                end,
                ltl_orders_ids = case
                    when
                        old.id_order_type = 2
                    then
                        cm_array_remove_one_element(ltl_orders_ids, old.id)
                    else
                        ltl_orders_ids
                end
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
                fcl_orders_ids = case
                    when
                        new.id_order_type = 1
                        and
                        not(old.id_order_type = 1)
                    then
                        array_append(fcl_orders_ids, new.id)
                    when
                        not(new.id_order_type = 1)
                        and
                        old.id_order_type = 1
                    then
                        cm_array_remove_one_element(fcl_orders_ids, old.id)
                    else
                        fcl_orders_ids
                end,
                ltl_orders_ids = case
                    when
                        new.id_order_type = 2
                        and
                        not(old.id_order_type = 2)
                    then
                        array_append(ltl_orders_ids, new.id)
                    when
                        not(new.id_order_type = 2)
                        and
                        old.id_order_type = 2
                    then
                        cm_array_remove_one_element(ltl_orders_ids, old.id)
                    else
                        ltl_orders_ids
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            (
                old.id_order_type = 1
                or
                old.id_order_type = 2
            )
        then
            update companies set
                fcl_orders_ids = case
                    when
                        old.id_order_type = 1
                    then
                        cm_array_remove_one_element(fcl_orders_ids, old.id)
                    else
                        fcl_orders_ids
                end,
                ltl_orders_ids = case
                    when
                        old.id_order_type = 2
                    then
                        cm_array_remove_one_element(ltl_orders_ids, old.id)
                    else
                        ltl_orders_ids
                end
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            (
                new.id_order_type = 1
                or
                new.id_order_type = 2
            )
        then
            update companies set
                fcl_orders_ids = case
                    when
                        new.id_order_type = 1
                    then
                        array_append(fcl_orders_ids, new.id)
                    else
                        fcl_orders_ids
                end,
                ltl_orders_ids = case
                    when
                        new.id_order_type = 2
                    then
                        array_append(ltl_orders_ids, new.id)
                    else
                        ltl_orders_ids
                end
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_client is not null
            and
            (
                new.id_order_type = 1
                or
                new.id_order_type = 2
            )
        then
            update companies set
                fcl_orders_ids = case
                    when
                        new.id_order_type = 1
                    then
                        array_append(fcl_orders_ids, new.id)
                    else
                        fcl_orders_ids
                end,
                ltl_orders_ids = case
                    when
                        new.id_order_type = 2
                    then
                        array_append(ltl_orders_ids, new.id)
                    else
                        ltl_orders_ids
                end
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