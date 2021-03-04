create or replace function cache_managers_and_watchers_for_self_on_tasks()
returns trigger as $body$
declare new_totals record;
begin

    if TG_OP = 'UPDATE' then
        if
            new.orders_managers_ids is not distinct from old.orders_managers_ids
            and
            new.watchers_ids is not distinct from old.watchers_ids
        then
            return new;
        end if;
    end if;


    select
        new.watchers_ids || new.orders_managers_ids as watchers_or_managers
    into new_totals;

    if new_totals.watchers_or_managers is distinct from new.watchers_or_managers then

        update tasks set
            watchers_or_managers = new_totals.watchers_or_managers
        where
            public.tasks.id = new.id;

    end if;

    return new;
end
$body$
language plpgsql;

create trigger cache_managers_and_watchers_for_self_on_tasks
after insert or update of orders_managers_ids, watchers_ids
on public.tasks
for each row
execute procedure cache_managers_and_watchers_for_self_on_tasks();