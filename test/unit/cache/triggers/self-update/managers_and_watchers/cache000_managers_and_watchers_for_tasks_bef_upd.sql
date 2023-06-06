create or replace function cache000_managers_and_watchers_for_tasks_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if
        new.orders_managers_ids is not distinct from old.orders_managers_ids
        and
        new.watchers_ids is not distinct from old.watchers_ids
    then
        return new;
    end if;


    new.watchers_or_managers = new.watchers_ids || new.orders_managers_ids;

    return new;
end
$body$
language plpgsql;

create trigger cache000_managers_and_watchers_for_tasks_bef_upd
before update of orders_managers_ids, watchers_ids
on public.tasks
for each row
execute procedure cache000_managers_and_watchers_for_tasks_bef_upd();