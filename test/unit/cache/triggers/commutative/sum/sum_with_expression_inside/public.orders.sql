create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                orders_total = orders_total - coalesce(
                    (old.debet - old.credit) * old.quantity,
                    0
                )
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.credit is not distinct from old.credit
            and
            new.debet is not distinct from old.debet
            and
            new.id_client is not distinct from old.id_client
            and
            new.quantity is not distinct from old.quantity
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                orders_total = orders_total - coalesce(
                    (old.debet - old.credit) * old.quantity,
                    0
                ) + coalesce(
                    (new.debet - new.credit) * new.quantity,
                    0
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_total = orders_total - coalesce(
                    (old.debet - old.credit) * old.quantity,
                    0
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_total = orders_total + coalesce(
                    (new.debet - new.credit) * new.quantity,
                    0
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                orders_total = orders_total + coalesce(
                    (new.debet - new.credit) * new.quantity,
                    0
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
after insert or update of credit, debet, id_client, quantity or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();