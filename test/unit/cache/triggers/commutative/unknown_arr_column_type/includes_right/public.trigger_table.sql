create or replace function cache_totals_for_cache_table_on_trigger_table()
returns trigger as $body$
declare inserted_unknown_ids bigint[];
declare not_changed_unknown_ids bigint[];
declare deleted_unknown_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if old.unknown_ids is not null then
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
                        ARRAY[cache_table.id] :: bigint[] <@ source_row.unknown_ids
                )
            where
                cache_table.id = any( old.unknown_ids::bigint[] );
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
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
            'id', new.id,'profit', new.profit,'unknown_ids', new.unknown_ids
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
                null::jsonb,
                jsonb_build_object(
                'id', new.id,'profit', new.profit,'unknown_ids', new.unknown_ids
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.trigger_table, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        ARRAY[cache_table.id] :: bigint[] <@ source_row.unknown_ids
                )
            where
                cache_table.id = any( not_changed_unknown_ids::bigint[] );
        end if;

        if deleted_unknown_ids is not null then
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
                        ARRAY[cache_table.id] :: bigint[] <@ source_row.unknown_ids
                )
            where
                cache_table.id = any( deleted_unknown_ids::bigint[] );
        end if;

        if inserted_unknown_ids is not null then
            update cache_table set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
            'id', new.id,'profit', new.profit,'unknown_ids', new.unknown_ids
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
                null::jsonb,
                jsonb_build_object(
                'id', new.id,'profit', new.profit,'unknown_ids', new.unknown_ids
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.trigger_table, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        ARRAY[cache_table.id] :: bigint[] <@ source_row.unknown_ids
                )
            where
                cache_table.id = any( inserted_unknown_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.unknown_ids is not null then
            update cache_table set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
            'id', new.id,'profit', new.profit,'unknown_ids', new.unknown_ids
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
                null::jsonb,
                jsonb_build_object(
                'id', new.id,'profit', new.profit,'unknown_ids', new.unknown_ids
            ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.trigger_table, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        ARRAY[cache_table.id] :: bigint[] <@ source_row.unknown_ids
                )
            where
                cache_table.id = any( new.unknown_ids::bigint[] );
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