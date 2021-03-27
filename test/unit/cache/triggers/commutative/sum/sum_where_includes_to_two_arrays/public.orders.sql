create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_clients_ids integer[];
declare inserted_partners_ids integer[];
declare not_changed_clients_ids integer[];
declare not_changed_partners_ids integer[];
declare deleted_clients_ids integer[];
declare deleted_partners_ids integer[];
begin

    if TG_OP = 'DELETE' then

        if
            old.clients_ids is not null
            and
            old.partners_ids is not null
            and
            old.deleted = 0
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                companies.id = any( old.clients_ids )
                and
                companies.id = any( old.partners_ids );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            cm_equal_arrays(new.clients_ids, old.clients_ids)
            and
            new.deleted is not distinct from old.deleted
            and
            cm_equal_arrays(new.partners_ids, old.partners_ids)
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
            inserted_clients_ids = null;
            inserted_partners_ids = null;
            not_changed_clients_ids = null;
            not_changed_partners_ids = null;
            deleted_clients_ids = old.clients_ids;
            deleted_partners_ids = old.partners_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_clients_ids = new.clients_ids;
            inserted_partners_ids = new.partners_ids;
            not_changed_clients_ids = null;
            not_changed_partners_ids = null;
            deleted_clients_ids = null;
            deleted_partners_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_clients_ids = cm_get_inserted_elements(old.clients_ids, new.clients_ids);
            inserted_partners_ids = cm_get_inserted_elements(old.partners_ids, new.partners_ids);
            not_changed_clients_ids = cm_get_not_changed_elements(old.clients_ids, new.clients_ids);
            not_changed_partners_ids = cm_get_not_changed_elements(old.partners_ids, new.partners_ids);
            deleted_clients_ids = cm_get_deleted_elements(old.clients_ids, new.clients_ids);
            deleted_partners_ids = cm_get_deleted_elements(old.partners_ids, new.partners_ids);
        end if;

        if
            not_changed_clients_ids is not null
            and
            not_changed_partners_ids is not null
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                companies.id = any( not_changed_clients_ids )
                and
                companies.id = any( not_changed_partners_ids );
        end if;

        if
            deleted_clients_ids is not null
            and
            deleted_partners_ids is not null
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                companies.id = any( deleted_clients_ids )
                and
                companies.id = any( deleted_partners_ids );
        end if;

        if
            inserted_clients_ids is not null
            and
            inserted_partners_ids is not null
        then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                companies.id = any( inserted_clients_ids )
                and
                companies.id = any( inserted_partners_ids );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.clients_ids is not null
            and
            new.partners_ids is not null
            and
            new.deleted = 0
        then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                companies.id = any( new.clients_ids )
                and
                companies.id = any( new.partners_ids );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of clients_ids, deleted, partners_ids, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();