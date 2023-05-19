create or replace function cache000_log_oper_id2_bef_ins()
returns trigger as $body$
begin
    new.id2 = new.id * 2;

    return new;
end
$body$
language plpgsql;

create trigger cache000_log_oper_id2_bef_ins
before insert
on public.log_oper
for each row
execute procedure cache000_log_oper_id2_bef_ins();