create or replace function cache000_self_dates_for_list_gtd_bef_ins()
returns trigger as $body$
begin
    new.clear_date_total = coalesce(
        new.date_clear,
        new.date_conditional_clear,
        new.date_release_for_procuring
    );

    return new;
end
$body$
language plpgsql;

create trigger cache000_self_dates_for_list_gtd_bef_ins
before insert
on public.list_gtd
for each row
execute procedure cache000_self_dates_for_list_gtd_bef_ins();