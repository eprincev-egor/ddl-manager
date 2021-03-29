create or replace function cache_totals_for_cache_table_on_trigger_table()
returns trigger as $body$
declare inserted_unknown_ids bigint[];
declare not_changed_unknown_ids bigint[];
declare deleted_unknown_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if old.unknown_ids is not null then
            update cache_table set
                total_profit = total_profit - coalesce(old.profit, 0)
            where
                cache_table.id = any (old.unknown_ids);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.profit is not distinct from old.profit
            and
            new.unknown_ids is not distinct from old.unknown_ids
        then
            return new;
        end if;

        inserted_unknown_ids = cm_get_inserted_elements(old.unknown_ids, new.unknown_ids);
        not_changed_unknown_ids = cm_get_not_changed_elements(old.unknown_ids, new.unknown_ids);
        deleted_unknown_ids = cm_get_deleted_elements(old.unknown_ids, new.unknown_ids);

        if not_changed_unknown_ids is not null then
            update cache_table set
                total_profit = total_profit - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                cache_table.id = any (not_changed_unknown_ids);
        end if;

        if deleted_unknown_ids is not null then
            update cache_table set
                total_profit = total_profit - coalesce(old.profit, 0)
            where
                cache_table.id = any (deleted_unknown_ids);
        end if;

        if inserted_unknown_ids is not null then
            update cache_table set
                total_profit = total_profit + coalesce(new.profit, 0)
            where
                cache_table.id = any (inserted_unknown_ids);
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.unknown_ids is not null then
            update cache_table set
                total_profit = total_profit + coalesce(new.profit, 0)
            where
                cache_table.id = any (new.unknown_ids);
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_cache_table_on_trigger_table
after insert or update of profit, unknown_ids or delete
on public.trigger_table
for each row
execute procedure cache_totals_for_cache_table_on_trigger_table();