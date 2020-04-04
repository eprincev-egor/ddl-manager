create function test_func(id bigint default null)
returns void as $body$
    begin
        raise notice 'test 1';
    end
$body$
language plpgsql;