create or replace function cm_array_append_order_by_asc_nulls_last(
    input_arr anyarray,
    element_to_append anyelement
)
returns anyarray as $body$
declare element_position integer;
begin
    if array_length(input_arr, 1) is null then
        return array[ element_to_append ];
    end if;

    return (
        select
            array_agg(
                array_value
                order by array_value asc
                nulls last
            )
        from unnest( input_arr || element_to_append ) as array_value
    );
end
$body$
language plpgsql;