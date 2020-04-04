create table company (
    id serial primary key
);

create function some_func()
returns public.company as $body$
begin
end
$body$
language plpgsql;