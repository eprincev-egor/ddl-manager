create or replace function cache_last_comment_for_unit_on_comments()
returns trigger as $body$
declare prev_row record;
declare prev_id bigint;
begin

    if TG_OP = 'DELETE' then
        if old.unit_id is not null then
            if not old.__last_comment_for_unit then
                return old;
            end if;

            select
                id,
                message,
                unit_id
            from comments
            where
                comments.unit_id = old.unit_id
            order by
                comments.id desc nulls last
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update comments set
                    __last_comment_for_unit = true
                where
                    comments.id = prev_row.id;
            end if;

            update operation.unit set
                last_comment = prev_row.message
            where
                old.unit_id = operation.unit.id
                and
                operation.unit.last_comment is distinct from prev_row.message;

        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.message is not distinct from old.message
            and
            new.unit_id is not distinct from old.unit_id
        then
            return new;
        end if;

        if new.unit_id is not distinct from old.unit_id then
            if not new.__last_comment_for_unit then
                return new;
            end if;

            update operation.unit set
                last_comment = new.message
            where
                new.unit_id = operation.unit.id
                and
                operation.unit.last_comment is distinct from new.message;

            return new;
        end if;

        if
            old.unit_id is not null
            and
            old.__last_comment_for_unit
        then
            select
                id,
                message,
                unit_id
            from comments
            where
                comments.unit_id = old.unit_id
            order by
                comments.id desc nulls last
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update comments set
                    __last_comment_for_unit = true
                where
                    comments.id = prev_row.id;
            end if;

            update operation.unit set
                last_comment = prev_row.message
            where
                old.unit_id = operation.unit.id
                and
                operation.unit.last_comment is distinct from prev_row.message;
        end if;

        if new.unit_id is not null then
            prev_id = (
                select
                    max( comments.id )
                from comments
                where
                    comments.unit_id = new.unit_id
                    and
                    comments.id <> new.id
            );

            if
                prev_id < new.id
                or
                prev_id is null
            then
                if prev_id is not null then
                    update comments set
                        __last_comment_for_unit = false
                    where
                        comments.id = prev_id
                        and
                        __last_comment_for_unit = true;
                end if;

                if not new.__last_comment_for_unit then
                    update comments set
                        __last_comment_for_unit = true
                    where
                        comments.id = new.id;
                end if;

                update operation.unit set
                    last_comment = new.message
                where
                    new.unit_id = operation.unit.id
                    and
                    operation.unit.last_comment is distinct from new.message;
            end if;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if new.unit_id is not null then
            update comments set
                __last_comment_for_unit = false
            where
                comments.unit_id = new.unit_id
                and
                comments.id < new.id
                and
                __last_comment_for_unit = true;

            update operation.unit set
                last_comment = new.message
            where
                new.unit_id = operation.unit.id
                and
                operation.unit.last_comment is distinct from new.message;

        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_last_comment_for_unit_on_comments
after insert or update of message, unit_id or delete
on public.comments
for each row
execute procedure cache_last_comment_for_unit_on_comments();