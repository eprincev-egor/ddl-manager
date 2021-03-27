create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_companies_ids integer[];
declare not_changed_companies_ids integer[];
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
                ARRAY[companies.id] @> old.companies_ids;
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

        matched_old = coalesce(old.deleted = 0, false);
        matched_new = coalesce(new.deleted = 0, false);

        if
            not matched_old
            and
            not matched_new
        then
            return new;
        end if;

        if
            matched_old
            and
            not matched_new
        then
            inserted_companies_ids = null;
            not_changed_companies_ids = null;
            deleted_companies_ids = old.companies_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_companies_ids = new.companies_ids;
            not_changed_companies_ids = null;
            deleted_companies_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_companies_ids = cm_get_inserted_elements(old.companies_ids, new.companies_ids);
            not_changed_companies_ids = cm_get_not_changed_elements(old.companies_ids, new.companies_ids);
            deleted_companies_ids = cm_get_deleted_elements(old.companies_ids, new.companies_ids);
        end if;

        if not_changed_companies_ids is not null then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                ARRAY[companies.id] @> not_changed_companies_ids;
        end if;

        if deleted_companies_ids is not null then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                ARRAY[companies.id] @> deleted_companies_ids;
        end if;

        if inserted_companies_ids is not null then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                ARRAY[companies.id] @> inserted_companies_ids;
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
                ARRAY[companies.id] @> new.companies_ids;
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