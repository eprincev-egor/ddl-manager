create or replace function cache000_gtd_for_comments_bef_ins()
returns trigger as $body$
declare new_totals record;
begin


    if not coalesce(new.query_name in ('LIST_ALL_GTD', 'LIST_ARCHIVE_GTD', 'LIST_ACTIVE_GTD', 'LIST_GTD'), false) then
        return new;
    end if;


    select
            gtd.orders_ids [1] * 3 as gtd_order_id
    from list_gtd as gtd
    where
        gtd.id = new.row_id
        and
        gtd.deleted = 0
        and
        array_length_excluding_nulls(gtd.orders_ids) = 1
        and
        new.query_name in ('LIST_ALL_GTD', 'LIST_ARCHIVE_GTD', 'LIST_ACTIVE_GTD', 'LIST_GTD')
    into new_totals;


    new.gtd_order_id = new_totals.gtd_order_id;


    return new;
end
$body$
language plpgsql;

create trigger cache000_gtd_for_comments_bef_ins
before insert
on public.comments
for each row
execute procedure cache000_gtd_for_comments_bef_ins();