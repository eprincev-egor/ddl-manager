create or replace function cache_user_task_for_comments_on_user_task()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if old.deleted = 0 then
            update comments set
                target_query_name = coalesce(
                    (null::text),
                    comments.query_name
                ),
                target_row_id = coalesce(
                    (null::int8),
                    comments.row_id
                )
            where
                old.id = comments.row_id
                and
                comments.query_name = 'USER_TASK'
                and
                (
                    comments.target_query_name is distinct from coalesce(
                        (null::text),
                        comments.query_name
                    )
                    or
                    comments.target_row_id is distinct from coalesce(
                        (null::int8),
                        comments.row_id
                    )
                );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.query_name is not distinct from old.query_name
            and
            new.row_id is not distinct from old.row_id
        then
            return new;
        end if;

        if
            coalesce(old.deleted = 0, false)
            or
            coalesce(new.deleted = 0, false)
        then
            update comments set
                target_query_name = case
                    when
                        coalesce(new.deleted = 0, false)
                    then
                        coalesce(
                            new.query_name,
                            comments.query_name
                        )
                    else
                        null
                end,
                target_row_id = case
                    when
                        coalesce(new.deleted = 0, false)
                    then
                        coalesce(
                            new.row_id,
                            comments.row_id
                        )
                    else
                        null
                end
            where
                new.id = comments.row_id
                and
                comments.query_name = 'USER_TASK'
                and
                (
                    comments.target_query_name is distinct from case
                        when
                            coalesce(new.deleted = 0, false)
                        then
                            coalesce(
                                new.query_name,
                                comments.query_name
                            )
                        else
                            null
                    end
                    or
                    comments.target_row_id is distinct from case
                        when
                            coalesce(new.deleted = 0, false)
                        then
                            coalesce(
                                new.row_id,
                                comments.row_id
                            )
                        else
                            null
                    end
                );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_user_task_for_comments_on_user_task
after update of deleted, query_name, row_id or delete
on public.user_task
for each row
execute procedure cache_user_task_for_comments_on_user_task();