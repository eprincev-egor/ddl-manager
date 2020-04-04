create function test_func_sql()
returns integer as $body$select 1$body$
language sql;