create or replace function cache_gtd_for_comments_on_list_gtd()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if
            old.orders_ids [1] * 3 is not null
            and
            old.deleted = 0
            and
            array_length_excluding_nulls(old.orders_ids) = 1
        then
            update comments set
                gtd_order_id = (null::bigint[]) [1] * 3
            where
                old.id = comments.row_id
                and
                comments.query_name in ('LIST_ALL_GTD', 'LIST_ARCHIVE_GTD', 'LIST_ACTIVE_GTD', 'LIST_GTD')
                and
                comments.gtd_order_id is distinct from (null::bigint[]) [1] * 3;
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
            and
            coalesce(array_length_excluding_nulls(old.orders_ids) = 1, false)
            or
            coalesce(new.deleted = 0, false)
            and
            coalesce(array_length_excluding_nulls(new.orders_ids) = 1, false)
        then
            update comments set
                gtd_order_id = new.orders_ids [1] * 3
            where
                new.id = comments.row_id
                and
                comments.query_name in ('LIST_ALL_GTD', 'LIST_ARCHIVE_GTD', 'LIST_ACTIVE_GTD', 'LIST_GTD')
                and
                comments.gtd_order_id is distinct from new.orders_ids [1] * 3;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.orders_ids [1] * 3 is not null
            and
            new.deleted = 0
            and
            array_length_excluding_nulls(new.orders_ids) = 1
        then
            update comments set
                gtd_order_id = new.orders_ids [1] * 3
            where
                new.id = comments.row_id
                and
                comments.query_name in ('LIST_ALL_GTD', 'LIST_ARCHIVE_GTD', 'LIST_ACTIVE_GTD', 'LIST_GTD')
                and
                comments.gtd_order_id is distinct from new.orders_ids [1] * 3;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_gtd_for_comments_on_list_gtd
after insert or update of deleted, orders_ids or delete
on public.list_gtd
for each row
execute procedure cache_gtd_for_comments_on_list_gtd();