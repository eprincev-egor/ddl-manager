create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            (
                old.id_client is not null
                or
                old.id_partner is not null
            )
            and
            old.deleted = 0
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                companies.id in (old.id_client, old.id_partner);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_client is not distinct from old.id_client
            and
            new.id_partner is not distinct from old.id_partner
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if
            new.id_client is not distinct from old.id_client
            and
            new.id_partner is not distinct from old.id_partner
            and
            new.deleted is not distinct from old.deleted
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                companies.id in (new.id_client, new.id_partner);

            return new;
        end if;

        if
            (
                old.id_client is not null
                or
                old.id_partner is not null
            )
            and
            old.deleted = 0
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                companies.id in (old.id_client, old.id_partner);
        end if;

        if
            (
                new.id_client is not null
                or
                new.id_partner is not null
            )
            and
            new.deleted = 0
        then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                companies.id in (new.id_client, new.id_partner);
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            (
                new.id_client is not null
                or
                new.id_partner is not null
            )
            and
            new.deleted = 0
        then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                companies.id in (new.id_client, new.id_partner);
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of deleted, id_client, id_partner, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();