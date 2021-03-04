create or replace function cache_gtd_without_insert_for_comments_on_list_gtd()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if
            old.orders_ids is not null
            and
            old.deleted = 0
        then
            update comments set
                gtd_orders_ids = (null::bigint[])
            where
                old.id = comments.row_id
                and
                comments.query_name in ('LIST_ALL_GTD', 'LIST_ARCHIVE_GTD', 'LIST_ACTIVE_GTD', 'LIST_GTD');
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

        if
            coalesce(old.deleted = 0, false)
            or
            coalesce(new.deleted = 0, false)
        then
            update comments set
                gtd_orders_ids = new.orders_ids
            where
                new.id = comments.row_id
                and
                comments.query_name in ('LIST_ALL_GTD', 'LIST_ARCHIVE_GTD', 'LIST_ACTIVE_GTD', 'LIST_GTD');
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_gtd_without_insert_for_comments_on_list_gtd
after update of deleted, orders_ids or delete
on public.list_gtd
for each row
execute procedure cache_gtd_without_insert_for_comments_on_list_gtd();