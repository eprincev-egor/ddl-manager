create function func_1(arg_1 bigint)
returns integer as $body$
    begin
        raise notice 'test 1';
    end
$body$
language plpgsql;

create function func_1(arg_1 text)
returns integer as $body$
    begin
        raise notice 'test 2';
    end
$body$
language plpgsql;