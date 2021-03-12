create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                orders_count_id_partner = cm_array_remove_one_element(
                    orders_count_id_partner,
                    old.id_partner
                ),
                orders_count = (
                    select
                        count(distinct item.id_partner)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_count_id_partner,
                            old.id_partner
                        )
                    ) as item(id_partner)
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
            new.id_partner is not distinct from old.id_partner
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            if new.id_client is null then
                return new;
            end if;

            update companies set
                orders_count_id_partner = array_append(
                    cm_array_remove_one_element(
                        orders_count_id_partner,
                        old.id_partner
                    ),
                    new.id_partner
                ),
                orders_count = (
                    select
                        count(distinct item.id_partner)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                orders_count_id_partner,
                                old.id_partner
                            ),
                            new.id_partner
                        )
                    ) as item(id_partner)
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_count_id_partner = cm_array_remove_one_element(
                    orders_count_id_partner,
                    old.id_partner
                ),
                orders_count = (
                    select
                        count(distinct item.id_partner)

                    from unnest(
                        cm_array_remove_one_element(
                            orders_count_id_partner,
                            old.id_partner
                        )
                    ) as item(id_partner)
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_count_id_partner = array_append(
                    orders_count_id_partner,
                    new.id_partner
                ),
                orders_count = (
                    select
                        count(distinct item.id_partner)

                    from unnest(
                        array_append(
                            orders_count_id_partner,
                            new.id_partner
                        )
                    ) as item(id_partner)
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                orders_count_id_partner = array_append(
                    orders_count_id_partner,
                    new.id_partner
                ),
                orders_count = (
                    select
                        count(distinct item.id_partner)

                    from unnest(
                        array_append(
                            orders_count_id_partner,
                            new.id_partner
                        )
                    ) as item(id_partner)
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
after insert or update of id_client, id_partner or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();