create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare old_vat_vat_value numeric;
declare new_vat_vat_value numeric;
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            if old.id_vat is not null then
                old_vat_vat_value = (
                    select
                        vats.vat_value
                    from vats
                    where
                        vats.id = old.id_vat
                );
            end if;

            update companies set
                orders_total = orders_total - coalesce(
                    old.profit * old_vat_vat_value,
                    0
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
            new.id_vat is not distinct from old.id_vat
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if old.id_vat is not null then
            old_vat_vat_value = (
                select
                    vats.vat_value
                from vats
                where
                    vats.id = old.id_vat
            );
        end if;

        if new.id_vat is not distinct from old.id_vat then
            new_vat_vat_value = old_vat_vat_value;
        else
            if new.id_vat is not null then
                new_vat_vat_value = (
                    select
                        vats.vat_value
                    from vats
                    where
                        vats.id = new.id_vat
                );
            end if;
        end if;

        if new.id_client is not distinct from old.id_client then
            if new.id_client is null then
                return new;
            end if;

            update companies set
                orders_total = orders_total - coalesce(
                    old.profit * old_vat_vat_value,
                    0
                ) + coalesce(
                    new.profit * new_vat_vat_value,
                    0
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_total = orders_total - coalesce(
                    old.profit * old_vat_vat_value,
                    0
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_total = orders_total + coalesce(
                    new.profit * new_vat_vat_value,
                    0
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            if new.id_vat is not null then
                new_vat_vat_value = (
                    select
                        vats.vat_value
                    from vats
                    where
                        vats.id = new.id_vat
                );
            end if;

            update companies set
                orders_total = orders_total + coalesce(
                    new.profit * new_vat_vat_value,
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
after insert or update of id_client, id_vat, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();