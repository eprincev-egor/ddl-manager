create or replace function cache_totals_for_gtd_on_units()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_orders_ids bigint[];
declare deleted_orders_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if
            old.orders_ids is not null
            and
            old.deleted = 0
        then
            update gtd set
                units_count = units_count - 1
            where
                old.orders_ids && gtd.orders_ids;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            cm_equal_arrays(new.orders_ids, old.orders_ids)
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
            inserted_orders_ids = null;
            deleted_orders_ids = old.orders_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_orders_ids = new.orders_ids;
            deleted_orders_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_orders_ids = cm_get_inserted_elements(old.orders_ids, new.orders_ids);
            deleted_orders_ids = cm_get_deleted_elements(old.orders_ids, new.orders_ids);
        end if;


        if deleted_orders_ids is not null then
            update gtd set
                units_count = units_count - 1
            where
                deleted_orders_ids && gtd.orders_ids;
        end if;

        if inserted_orders_ids is not null then
            update gtd set
                units_count = units_count + 1
            where
                inserted_orders_ids && gtd.orders_ids;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.orders_ids is not null
            and
            new.deleted = 0
        then
            update gtd set
                units_count = units_count + 1
            where
                new.orders_ids && gtd.orders_ids;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_gtd_on_units
after insert or update of deleted, orders_ids or delete
on public.units
for each row
execute procedure cache_totals_for_gtd_on_units();