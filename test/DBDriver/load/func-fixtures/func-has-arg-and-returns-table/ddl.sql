create function test_func( nice text )
returns table(x text, y integer) as $body$
    begin
        raise notice 'test 1';
    end
$body$
language plpgsql;