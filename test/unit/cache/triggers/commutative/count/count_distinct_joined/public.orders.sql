create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare old_order_type_name text;
declare new_order_type_name text;
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            if old.id_order_type is not null then
                old_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = old.id_order_type
                );
            end if;

            update companies set
                orders_count_name = cm_array_remove_one_element(
                    orders_count_name,
                    old_order_type_name
                ),
                orders_count = (
                    select
                        count(distinct item.name)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_count_name,
                            old_order_type_name
                        )
                    ) as item(name)
                )
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

        if old.id_order_type is not null then
            old_order_type_name = (
                select
                    order_type.name
                from order_type
                where
                    order_type.id = old.id_order_type
            );
        end if;

        if new.id_order_type is not distinct from old.id_order_type then
            new_order_type_name = old_order_type_name;
        else
            if new.id_order_type is not null then
                new_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = new.id_order_type
                );
            end if;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                orders_count_name = array_append(
                    cm_array_remove_one_element(
                        orders_count_name,
                        old_order_type_name
                    ),
                    new_order_type_name
                ),
                orders_count = (
                    select
                        count(distinct item.name)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                orders_count_name,
                                old_order_type_name
                            ),
                            new_order_type_name
                        )
                    ) as item(name)
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_count_name = cm_array_remove_one_element(
                    orders_count_name,
                    old_order_type_name
                ),
                orders_count = (
                    select
                        count(distinct item.name)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_count_name,
                            old_order_type_name
                        )
                    ) as item(name)
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_count_name = array_append(
                    orders_count_name,
                    new_order_type_name
                ),
                orders_count = (
                    select
                        count(distinct item.name)

                    from unnest(
                        array_append(
                            orders_count_name,
                            new_order_type_name
                        )
                    ) as item(name)
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            if new.id_order_type is not null then
                new_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = new.id_order_type
                );
            end if;

            update companies set
                orders_count_name = array_append(
                    orders_count_name,
                    new_order_type_name
                ),
                orders_count = (
                    select
                        count(distinct item.name)

                    from unnest(
                        array_append(
                            orders_count_name,
                            new_order_type_name
                        )
                    ) as item(name)
                )
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