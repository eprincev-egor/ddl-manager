create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare inserted_companies_ids integer[];
declare deleted_companies_ids integer[];
begin

    if TG_OP = 'DELETE' then

        if
            old.companies_ids is not null
            and
            old.deleted = 0
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                old.companies_ids <@ array[companies.id];
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            cm_equal_arrays(new.companies_ids, old.companies_ids)
            and
            new.deleted is not distinct from old.deleted
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        inserted_companies_ids = cm_get_inserted_elements(old.companies_ids, new.companies_ids);
        deleted_companies_ids = cm_get_deleted_elements(old.companies_ids, new.companies_ids);

        if
            cm_equal_arrays(new.companies_ids, old.companies_ids)
            and
            new.deleted is not distinct from old.deleted
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                new.companies_ids <@ array[companies.id];

            return new;
        end if;

        if
            deleted_companies_ids is not null
            and
            old.deleted = 0
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                deleted_companies_ids <@ array[companies.id];
        end if;

        if
            inserted_companies_ids is not null
            and
            new.deleted = 0
        then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                inserted_companies_ids <@ array[companies.id];
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.companies_ids is not null
            and
            new.deleted = 0
        then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                new.companies_ids <@ array[companies.id];
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of companies_ids, deleted, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();