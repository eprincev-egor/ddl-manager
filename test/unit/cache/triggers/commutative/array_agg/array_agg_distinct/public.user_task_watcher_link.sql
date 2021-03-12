create or replace function cache_watchers_for_user_task_on_user_task_watcher_link()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_user_task is not null then
            update user_task set
                watchers_users_ids_id_user = cm_array_remove_one_element(
                    watchers_users_ids_id_user,
                    old.id_user
                ),
                watchers_users_ids = (
                    select
                        array_agg(distinct item.id_user)

                    from unnest(
                        cm_array_remove_one_element(
                            watchers_users_ids_id_user,
                            old.id_user
                        )
                    ) as item(id_user)
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
                watchers_users_ids_id_user = array_append(
                    cm_array_remove_one_element(
                        watchers_users_ids_id_user,
                        old.id_user
                    ),
                    new.id_user
                ),
                watchers_users_ids = (
                    select
                        array_agg(distinct item.id_user)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                watchers_users_ids_id_user,
                                old.id_user
                            ),
                            new.id_user
                        )
                    ) as item(id_user)
                )
            where
                new.id_user_task = user_task.id;

            return new;
        end if;

        if old.id_user_task is not null then
            update user_task set
                watchers_users_ids_id_user = cm_array_remove_one_element(
                    watchers_users_ids_id_user,
                    old.id_user
                ),
                watchers_users_ids = (
                    select
                        array_agg(distinct item.id_user)

                    from unnest(
                        cm_array_remove_one_element(
                            watchers_users_ids_id_user,
                            old.id_user
                        )
                    ) as item(id_user)
                )
            where
                old.id_user_task = user_task.id;
        end if;

        if new.id_user_task is not null then
            update user_task set
                watchers_users_ids_id_user = array_append(
                    watchers_users_ids_id_user,
                    new.id_user
                ),
                watchers_users_ids = case
                    when
                        array_position(
                            watchers_users_ids,
                            new.id_user
                        )
                        is null
                    then
                        array_append(
                            watchers_users_ids,
                            new.id_user
                        )
                    else
                        watchers_users_ids
                end
            where
                new.id_user_task = user_task.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_user_task is not null then
            update user_task set
                watchers_users_ids_id_user = array_append(
                    watchers_users_ids_id_user,
                    new.id_user
                ),
                watchers_users_ids = case
                    when
                        array_position(
                            watchers_users_ids,
                            new.id_user
                        )
                        is null
                    then
                        array_append(
                            watchers_users_ids,
                            new.id_user
                        )
                    else
                        watchers_users_ids
                end
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