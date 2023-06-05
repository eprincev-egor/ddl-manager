create or replace function cache000_gtd_for_list_documents_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

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


    select
        gtd.orders_ids as gtd_orders_ids
    from list_gtd as gtd
    where
        gtd.id = new.table_id
        and
        gtd.deleted = 0
        and
        new.table_name in ('LIST_GTD_ACTIVE', 'LIST_GTD_ARCHIVE')
    into new_totals;


    new.gtd_orders_ids = new_totals.gtd_orders_ids;


    return new;
end
$body$
language plpgsql;

create trigger cache000_gtd_for_list_documents_bef_upd
before update of table_id, table_name
on public.list_documents
for each row
execute procedure cache000_gtd_for_list_documents_bef_upd();