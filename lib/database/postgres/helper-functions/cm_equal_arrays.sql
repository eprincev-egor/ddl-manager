create or replace function cm_equal_arrays(
    old_values anyarray,
    new_values anyarray
)
returns boolean as $body$
begin
    if
        new_values is null 
        and
        old_values is null
    then
        return true;
    end if;

    return (
        new_values @> old_values
        and
        new_values <@ old_values
    );
end
$body$
language plpgsql;