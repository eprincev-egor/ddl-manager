create or replace function cm_array_remove_elements(
    input_arr anyarray,
    elements_to_remove anyarray
)
returns anyarray as $body$
begin
    if 
        array_length(elements_to_remove, 1) is null 
        or
        array_length(input_arr, 1) is null 
    then
        return input_arr;
    end if;

    for i in 1..array_length(elements_to_remove, 1) loop
        input_arr = cm_array_remove_one_element(input_arr, elements_to_remove[i]);
    end loop;

    return input_arr;
end
$body$
language plpgsql;