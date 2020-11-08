import {
    wrapText,
    function2identifySql
} from "../../utils";

export function getCheckFreezeFunctionSql(func: any, errorText: any, actionOnFreeze = "error") {
    const funcIdentifySql = function2identifySql( func );
    let sqlOnFreeze;

    if ( actionOnFreeze === "error" ) {
        sqlOnFreeze = `raise exception ${wrapText(errorText)};`;
    }
    else if ( actionOnFreeze === "drop" ) {
        sqlOnFreeze = `drop function ${function2identifySql(func)};`;
    }
    else {
        sqlOnFreeze = actionOnFreeze;
    }

    return `
        do $$
            begin
                if exists(
                    select from information_schema.routines as routines
    
                    left join pg_namespace
                        on routines.routine_schema = pg_namespace.nspname
                    left join pg_proc
                        on pg_namespace.oid = pg_proc.pronamespace
                            and routines.routine_name = pg_proc.proname
    
                    where
                        routines.routine_schema = ${ wrapText(func.schema) } and
                        routines.routine_name = ${ wrapText(func.name) } and

                        coalesce(position(
                            'ddl-manager-sync'
                            in
                            pg_catalog.obj_description( pg_proc.oid )
                        ) > 0, false) = false

                        and

                        -- function identify
                        -- public.func_name(type,type)
                        -- without args who has default value
                        (routines.routine_schema || '.' || routines.routine_name || '(' || coalesce((
                            select
                                string_agg( type_name, ',' )
                            from (
                                select
                                    format_type(type_id::oid,NULL) as type_name
                                from unnest(
                                    string_to_array(proargtypes::text, ' ')
                                ) as type_id
                                
                                limit (
                                    array_length( string_to_array(proargtypes::text, ' '), 1 ) -
                                    length(regexp_replace(proargdefaults, '[^}]', '', 'g'))
                                )
                            ) as tmp
                        ),'') || ')')
                        =
                        ${wrapText(funcIdentifySql)}
                ) then
                    ${sqlOnFreeze}
                end if;
            end
        $$;
    `;
}