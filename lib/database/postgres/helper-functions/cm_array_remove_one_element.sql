create or replace function cm_array_remove_one_element(
    input_arr anyarray,
    element_to_remove anyelement
)
returns anyarray as $body$
declare element_position integer;
begin
    if input_arr is null then
        return input_arr;
    end if;

    element_position = array_position(
        input_arr,
        element_to_remove
    );

    return (
        input_arr[:(element_position - 1)] || 
        input_arr[(element_position + 1):]
    );
    
end
$body$
language plpgsql;