create or replace function cache_self_dates_for_self_on_list_gtd()
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


    select
        coalesce(
            new.date_clear,
            new.date_conditional_clear,
            new.date_release_for_procuring
        ) as clear_date_total
    into new_totals;

    if new_totals.clear_date_total is distinct from new.clear_date_total then

        new.clear_date_total = new_totals.clear_date_total;

    end if;

    return new;
end
$body$
language plpgsql;

create trigger cache_self_dates_for_self_on_list_gtd
before update of date_clear, date_conditional_clear, date_release_for_procuring
on public.list_gtd
for each row
execute procedure cache_self_dates_for_self_on_list_gtd();