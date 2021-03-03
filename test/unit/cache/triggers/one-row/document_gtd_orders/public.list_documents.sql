create or replace function cache_gtd_for_self_on_list_documents()
returns trigger as $body$
begin
    if TG_OP = 'INSERT' then
        if
            new.table_id is null
            and
            new.table_name is null
        then
            return new;
        end if;

        if not coalesce(new.table_name in ('LIST_GTD_ACTIVE', 'LIST_GTD_ARCHIVE'), false) then
            return new;
        end if;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.table_id is not distinct from old.table_id
            and
            new.table_name is not distinct from old.table_name
        then
            return new;
        end if;

        if
            not coalesce(old.table_name in ('LIST_GTD_ACTIVE', 'LIST_GTD_ARCHIVE'), false)
            and
            not coalesce(new.table_name in ('LIST_GTD_ACTIVE', 'LIST_GTD_ARCHIVE'), false)
        then
            return new;
        end if;
    end if;


    update list_documents set
        (
            gtd_orders_ids
        ) = (
            select
                gtd.orders_ids as gtd_orders_ids

            from list_gtd as gtd

            where
                gtd.id = list_documents.table_id
    and
    gtd.deleted = 0
    and
    list_documents.table_name in ('LIST_GTD_ACTIVE', 'LIST_GTD_ARCHIVE')
        )
    where
        public.list_documents.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_gtd_for_self_on_list_documents
after insert or update of table_id, table_name
on public.list_documents
for each row
execute procedure cache_gtd_for_self_on_list_documents();