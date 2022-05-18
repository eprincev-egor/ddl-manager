create or replace function cm_distinct_array(
    input_arr anyarray
)
returns anyarray as $body$
begin
    return (
        select array_agg(distinct item)
        from unnest( input_arr ) as item
    );
end
$body$
language plpgsql;