create table test (
    name text,
    note text
);

create function test_func()
returns trigger as $body$
begin
end
$body$
language plpgsql;


create trigger test_trigger
after insert or update of name, note or delete
on test
for each row
execute procedure test_func();