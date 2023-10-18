create or replace function cm_build_array_for(
    some_array anyarray,
    some_item bigint
)
returns anyarray as $body$
begin
    return ARRAY[ some_item ];
end
$body$ language plpgsql
immutable;

create or replace function cm_build_array_for(
    some_array anyarray,
    some_items bigint[]
)
returns anyarray as $body$
begin
    return some_items;
end
$body$ language plpgsql
immutable;

create or replace function cm_build_array_for(
    some_array anyarray,
    some_items integer[]
)
returns anyarray as $body$
begin
    return some_items;
end
$body$ language plpgsql
immutable;
