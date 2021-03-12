create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.deleted = 0 then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                unknown_func(
                    companies.id,
                    old.id_client
                );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_client is not distinct from old.id_client
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if
            new.id_client is not distinct from old.id_client
            and
            new.deleted is not distinct from old.deleted
        then
            if
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update companies set
                orders_total = orders_total - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                unknown_func(
                    companies.id,
                    new.id_client
                );

            return new;
        end if;

        if old.deleted = 0 then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                unknown_func(
                    companies.id,
                    old.id_client
                );
        end if;

        if new.deleted = 0 then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                unknown_func(
                    companies.id,
                    new.id_client
                );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.deleted = 0 then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                unknown_func(
                    companies.id,
                    new.id_client
                );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of deleted, id_client, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();