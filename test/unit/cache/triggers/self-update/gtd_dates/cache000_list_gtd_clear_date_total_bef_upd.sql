create or replace function cache000_self_dates_for_list_gtd_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if
        new.date_clear is not distinct from old.date_clear
        and
        new.date_conditional_clear is not distinct from old.date_conditional_clear
        and
        new.date_release_for_procuring is not distinct from old.date_release_for_procuring
    then
        return new;
    end if;


    new.clear_date_total = coalesce(
        new.date_clear,
        new.date_conditional_clear,
        new.date_release_for_procuring
        );

    return new;
end
$body$
language plpgsql;

create trigger cache000_self_dates_for_list_gtd_bef_upd
before update of date_clear, date_conditional_clear, date_release_for_procuring
on public.list_gtd
for each row
execute procedure cache000_self_dates_for_list_gtd_bef_upd();