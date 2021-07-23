create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            coalesce(old.id_order_type in (1, 2, 3), false)
        then
            update companies set
                orders_count = orders_count - 1
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
            if new.id_client is null then
                return new;
            end if;

            update companies set
                orders_count = case
                    when
                        new.id_order_type in (1, 2, 3)
                        and
                        not coalesce(old.id_order_type in (1, 2, 3), false)
                    then
                        orders_count + 1
                    when
                        not coalesce(new.id_order_type in (1, 2, 3), false)
                        and
                        old.id_order_type in (1, 2, 3)
                    then
                        orders_count - 1
                    else
                        orders_count
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            coalesce(old.id_order_type in (1, 2, 3), false)
        then
            update companies set
                orders_count = orders_count - 1
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            coalesce(new.id_order_type in (1, 2, 3), false)
        then
            update companies set
                orders_count = orders_count + 1
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_client is not null
            and
            coalesce(new.id_order_type in (1, 2, 3), false)
        then
            update companies set
                orders_count = orders_count + 1
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