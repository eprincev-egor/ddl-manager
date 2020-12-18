create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                all_orders_is_lcl_is_lcl = cm_array_remove_one_element(
                    all_orders_is_lcl_is_lcl,
                    old.is_lcl
                ),
                all_orders_is_lcl = (
                    select
                        bool_and(item.is_lcl)

                    from unnest(
                        cm_array_remove_one_element(
                            all_orders_is_lcl_is_lcl,
                            old.is_lcl
                        )
                    ) as item(is_lcl)
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
            new.is_lcl is not distinct from old.is_lcl
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                all_orders_is_lcl_is_lcl = array_append(
                    cm_array_remove_one_element(
                        all_orders_is_lcl_is_lcl,
                        old.is_lcl
                    ),
                    new.is_lcl
                ),
                all_orders_is_lcl = (
                    select
                        bool_and(item.is_lcl)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                all_orders_is_lcl_is_lcl,
                                old.is_lcl
                            ),
                            new.is_lcl
                        )
                    ) as item(is_lcl)
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                all_orders_is_lcl_is_lcl = cm_array_remove_one_element(
                    all_orders_is_lcl_is_lcl,
                    old.is_lcl
                ),
                all_orders_is_lcl = (
                    select
                        bool_and(item.is_lcl)

                    from unnest(
                        cm_array_remove_one_element(
                            all_orders_is_lcl_is_lcl,
                            old.is_lcl
                        )
                    ) as item(is_lcl)
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                all_orders_is_lcl_is_lcl = array_append(
                    all_orders_is_lcl_is_lcl,
                    new.is_lcl
                ),
                all_orders_is_lcl = all_orders_is_lcl
                and
                new.is_lcl
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                all_orders_is_lcl_is_lcl = array_append(
                    all_orders_is_lcl_is_lcl,
                    new.is_lcl
                ),
                all_orders_is_lcl = all_orders_is_lcl
                and
                new.is_lcl
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, is_lcl or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();