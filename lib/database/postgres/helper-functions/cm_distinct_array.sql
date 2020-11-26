create or replace function cm_distinct_array(
    input_arr anyarray
)
returns anyarray as $body$
begin
    return (
        select 
            array_agg(distinct input_value)
        from unnest( input_arr ) as input_value
    );
end
$body$
language plpgsql;