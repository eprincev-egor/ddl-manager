create or replace function cm_get_not_changed_elements(
    old_values anyarray,
    new_values anyarray
)
returns anyarray as $body$
begin
    return (
        select
            array_agg( distinct new_value )
        from unnest( new_values ) as new_value
        where
            exists(
                select from unnest( old_values ) as old_value
                where
                    new_value = old_value
            )
    );
end
$body$
language plpgsql;