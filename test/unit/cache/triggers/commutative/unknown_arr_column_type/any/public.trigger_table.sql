create or replace function cache_totals_for_cache_table_on_trigger_table()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update cache_table set
            __totals_json__ = __totals_json__ - old.id::text,
            (
                total_profit
            ) = (
                select
                        sum(source_row.profit) as total_profit
                from (
                    select
                            record.*
                    from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                    left join lateral jsonb_populate_record(null::public.trigger_table, json_entry.value) as record on
                        true
                ) as source_row
                where
                    source_row.id = any(cache_table.unknown_ids)
            )
        where
            cache_table.unknown_ids && cm_build_array_for((
                        select unknown_ids
                        from public.cache_table
                        where false
                    ), old.id);

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.profit is not distinct from old.profit then
            return new;
        end if;

        update cache_table set
            __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'id', new.id,'profit', new.profit
            ),
            TG_OP
        ),
            (
                total_profit
            ) = (
                select
                        sum(source_row.profit) as total_profit
                from (
                    select
                            record.*
                    from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'id', new.id,'profit', new.profit
                ),
                TG_OP
            )
) as json_entry

                    left join lateral jsonb_populate_record(null::public.trigger_table, json_entry.value) as record on
                        true
                ) as source_row
                where
                    source_row.id = any(cache_table.unknown_ids)
            )
        where
            cache_table.unknown_ids && cm_build_array_for((
                        select unknown_ids
                        from public.cache_table
                        where false
                    ), new.id);



        return new;
    end if;
end
$body$
language plpgsql;

create trigger cache_totals_for_cache_table_on_trigger_table
after update of profit or delete
on public.trigger_table
for each row
execute procedure cache_totals_for_cache_table_on_trigger_table();