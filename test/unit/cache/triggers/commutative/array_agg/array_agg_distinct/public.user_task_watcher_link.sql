create or replace function cache_watchers_for_user_task_on_user_task_watcher_link()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_user_task is not null then
            update user_task set
                __watchers_json__ = __watchers_json__ - old.id::text,
                (
                    watchers_users_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_user) as watchers_users_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __watchers_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.user_task_watcher_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_user_task = user_task.id
                )
            where
                old.id_user_task = user_task.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_user is not distinct from old.id_user
            and
            new.id_user_task is not distinct from old.id_user_task
        then
            return new;
        end if;

        if new.id_user_task is not distinct from old.id_user_task then
            if new.id_user_task is null then
                return new;
            end if;

            update user_task set
                __watchers_json__ = cm_merge_json(
            __watchers_json__,
            jsonb_build_object(
                'id', new.id,'id_user', new.id_user,'id_user_task', new.id_user_task
            ),
            TG_OP
        ),
                (
                    watchers_users_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_user) as watchers_users_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __watchers_json__,
                jsonb_build_object(
                    'id', new.id,'id_user', new.id_user,'id_user_task', new.id_user_task
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.user_task_watcher_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_user_task = user_task.id
                )
            where
                new.id_user_task = user_task.id;

            return new;
        end if;

        if old.id_user_task is not null then
            update user_task set
                __watchers_json__ = __watchers_json__ - old.id::text,
                (
                    watchers_users_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_user) as watchers_users_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __watchers_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.user_task_watcher_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_user_task = user_task.id
                )
            where
                old.id_user_task = user_task.id;
        end if;

        if new.id_user_task is not null then
            update user_task set
                __watchers_json__ = cm_merge_json(
            __watchers_json__,
            jsonb_build_object(
                'id', new.id,'id_user', new.id_user,'id_user_task', new.id_user_task
            ),
            TG_OP
        ),
                (
                    watchers_users_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_user) as watchers_users_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __watchers_json__,
                jsonb_build_object(
                    'id', new.id,'id_user', new.id_user,'id_user_task', new.id_user_task
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.user_task_watcher_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_user_task = user_task.id
                )
            where
                new.id_user_task = user_task.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_user_task is not null then
            update user_task set
                __watchers_json__ = cm_merge_json(
            __watchers_json__,
            jsonb_build_object(
                'id', new.id,'id_user', new.id_user,'id_user_task', new.id_user_task
            ),
            TG_OP
        ),
                (
                    watchers_users_ids
                ) = (
                    select
                            array_agg(distinct source_row.id_user) as watchers_users_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __watchers_json__,
                jsonb_build_object(
                    'id', new.id,'id_user', new.id_user,'id_user_task', new.id_user_task
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.user_task_watcher_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_user_task = user_task.id
                )
            where
                new.id_user_task = user_task.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_watchers_for_user_task_on_user_task_watcher_link
after insert or update of id_user, id_user_task or delete
on public.user_task_watcher_link
for each row
execute procedure cache_watchers_for_user_task_on_user_task_watcher_link();