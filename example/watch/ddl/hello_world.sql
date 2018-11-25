create or replace function hello_world()
returns void as $body$
begin
    -- YEAA!
    raise notice 'hello world';
end
$body$
language plpgsql;