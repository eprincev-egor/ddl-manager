create function some_func(text)
returns void as $body$
    begin
        raise notice 'test 1';
    end
$body$
language plpgsql;