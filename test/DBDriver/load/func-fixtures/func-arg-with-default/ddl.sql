create function test_func(id bigint default 1)
returns void as $body$
    begin
        raise notice 'test 1';
    end
$body$
language plpgsql;