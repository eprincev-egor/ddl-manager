create or replace function cache_gtd_for_list_documents_on_list_gtd()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if
            old.orders_ids is not null
            and
            old.deleted = 0
        then
            update list_documents set
                gtd_orders_ids = null
            where
                old.id = list_documents.table_id
                and
                list_documents.table_name in ('LIST_GTD_ACTIVE', 'LIST_GTD_ARCHIVE')
                and
                list_documents.gtd_orders_ids is distinct from null;
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
            update list_documents set
                gtd_orders_ids = case
                    when
                        coalesce(new.deleted = 0, false)
                    then
                        new.orders_ids
                    else
                        null
                end
            where
                new.id = list_documents.table_id
                and
                list_documents.table_name in ('LIST_GTD_ACTIVE', 'LIST_GTD_ARCHIVE')
                and
                list_documents.gtd_orders_ids is distinct from case
                    when
                        coalesce(new.deleted = 0, false)
                    then
                        new.orders_ids
                    else
                        null
                end;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.orders_ids is not null
            and
            new.deleted = 0
        then
            update list_documents set
                gtd_orders_ids = new.orders_ids
            where
                new.id = list_documents.table_id
                and
                list_documents.table_name in ('LIST_GTD_ACTIVE', 'LIST_GTD_ARCHIVE')
                and
                list_documents.gtd_orders_ids is distinct from new.orders_ids;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_gtd_for_list_documents_on_list_gtd
after insert or update of deleted, orders_ids or delete
on public.list_gtd
for each row
execute procedure cache_gtd_for_list_documents_on_list_gtd();