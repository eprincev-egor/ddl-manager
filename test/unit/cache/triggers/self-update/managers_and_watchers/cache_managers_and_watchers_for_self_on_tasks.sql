create or replace function cache_managers_and_watchers_for_self_on_tasks()
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

create trigger cache_managers_and_watchers_for_self_on_tasks
before update of orders_managers_ids, watchers_ids
on public.tasks
for each row
execute procedure cache_managers_and_watchers_for_self_on_tasks();