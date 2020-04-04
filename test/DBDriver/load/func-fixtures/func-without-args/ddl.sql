create function test_func()
returns void as $body$
    begin
        raise notice 'test 1';
    end
$body$
language plpgsql;