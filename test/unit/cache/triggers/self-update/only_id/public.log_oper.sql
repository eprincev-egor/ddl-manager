create or replace function cache_totals_for_self_on_log_oper()
returns trigger as $body$
declare new_totals record;
begin

    select
        new.id * 2 as id2
    into new_totals;

    if new_totals.id2 is distinct from new.id2 then

        update log_oper set
            id2 = new_totals.id2
        where
            public.log_oper.id = new.id;

    end if;

    return new;
end
$body$
language plpgsql;

create trigger cache_totals_for_self_on_log_oper
after insert
on public.log_oper
for each row
execute procedure cache_totals_for_self_on_log_oper();