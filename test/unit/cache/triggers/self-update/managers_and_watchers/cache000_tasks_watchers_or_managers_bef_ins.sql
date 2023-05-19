create or replace function cache000_tasks_watchers_or_managers_bef_ins()
returns trigger as $body$
begin
    new.watchers_or_managers = new.watchers_ids || new.orders_managers_ids;

    return new;
end
$body$
language plpgsql;

create trigger cache000_tasks_watchers_or_managers_bef_ins
before insert
on public.tasks
for each row
execute procedure cache000_tasks_watchers_or_managers_bef_ins();