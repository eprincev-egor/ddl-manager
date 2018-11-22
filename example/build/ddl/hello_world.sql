create or replace function hello_world()
returns void as $body$
begin
    raise notice 'hello world';
end
$body$
language plpgsql;