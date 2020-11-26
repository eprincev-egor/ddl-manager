create or replace function cm_array_to_string_distinct(
    input_arr text[],
    separator text
)
returns text as $body$
begin
    return (
        select 
            string_agg(distinct input_value, separator)
        from unnest( input_arr ) as input_value
    );
end
$body$
language plpgsql;