create function func_1(arg_1 bigint)
returns integer as $body$
    begin
        raise notice 'test 1';
    end
$body$
language plpgsql;

create function func_2(arg_2 text)
returns text as $body$
    begin
        raise notice 'test 2';
    end
$body$
language plpgsql;