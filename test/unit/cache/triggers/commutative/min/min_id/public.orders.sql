create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                min_order_id_id = cm_array_remove_one_element(min_order_id_id, old.id),
                min_order_id = (
                    select
                        min(item.id)

                    from unnest(
                        cm_array_remove_one_element(min_order_id_id, old.id)
                    ) as item(id)
                )
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.id_client is not distinct from old.id_client then
            return new;
        end if;



        if old.id_client is not null then
            update companies set
                min_order_id_id = cm_array_remove_one_element(min_order_id_id, old.id),
                min_order_id = (
                    select
                        min(item.id)

                    from unnest(
                        cm_array_remove_one_element(min_order_id_id, old.id)
                    ) as item(id)
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                min_order_id_id = array_append(min_order_id_id, new.id),
                min_order_id = (
                    select
                        min(item.id)

                    from unnest(
                        array_append(min_order_id_id, new.id)
                    ) as item(id)
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                min_order_id_id = array_append(min_order_id_id, new.id),
                min_order_id = (
                    select
                        min(item.id)

                    from unnest(
                        array_append(min_order_id_id, new.id)
                    ) as item(id)
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
after insert or update of id_client or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();