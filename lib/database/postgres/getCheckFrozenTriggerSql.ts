import {
    wrapText
} from "../../utils";

// TODO: any => type
export function getCheckFrozenTriggerSql(trigger: any, errorText: string) {
    return `
    do $$
        begin
            if exists(
                select from pg_trigger

                left join pg_class on 
                    pg_trigger.tgrelid = pg_class.oid

                left join pg_namespace ON
                    pg_namespace.oid = pg_class.relnamespace
                
                where
                    pg_trigger.tgname = ${ wrapText(trigger.name) } and
                    pg_namespace.nspname = ${ wrapText(trigger.table.schema) } and
                    pg_class.relname = ${ wrapText(trigger.table.name) } and
                    coalesce(position(
                        'ddl-manager-sync'
                        in
                        pg_catalog.obj_description(pg_trigger.oid) 
                    ) > 0, false) = false

            ) then
                raise exception ${wrapText( errorText )};
            end if;
        end
    $$;
    `;
}
