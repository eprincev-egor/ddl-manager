create function some_func(in id integer, out name text)
returns text as $body$
    begin
        raise notice 'test 1';
    end
$body$
language plpgsql;