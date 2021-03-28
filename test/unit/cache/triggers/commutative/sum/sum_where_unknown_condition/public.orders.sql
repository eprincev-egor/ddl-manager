create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update companies set
            orders_total = orders_total - coalesce(old.profit, 0)
        where
            unknown_func(
                companies.id,
                old.id_client
            );

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                unknown_func(
                    companies.id,
                    new.id_client
                );

            return new;
        end if;

        update companies set
            orders_total = orders_total - coalesce(old.profit, 0)
        where
            unknown_func(
                companies.id,
                old.id_client
            );

        update companies set
            orders_total = orders_total + coalesce(new.profit, 0)
        where
            unknown_func(
                companies.id,
                new.id_client
            );

        return new;
    end if;

    if TG_OP = 'INSERT' then

        update companies set
            orders_total = orders_total + coalesce(new.profit, 0)
        where
            unknown_func(
                companies.id,
                new.id_client
            );

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();