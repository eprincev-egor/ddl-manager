create or replace function cm_get_deleted_elements(
    old_values anyarray,
    new_values anyarray
)
returns anyarray as $body$
begin
    return (
        select
            array_agg( distinct old_value )
        from unnest( old_values ) as old_value
        where
            not exists(
                select from unnest( new_values ) as new_value
                where
                    new_value = old_value
            )
    );
end
$body$
language plpgsql;