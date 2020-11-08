import {
    findCommentByFunction,
    findCommentByTrigger,
    wrapText,

    function2identifySql,
    function2identifyJson,
    trigger2identifySql
} from "./utils";
import { Client } from "pg";
import { PostgresDriver } from "./database/PostgresDriver";
import { Comparator } from "./Comparator";

export class DbState {
    private db: Client;
    // TODO: any => type
    triggers: any[];
    functions: any[];
    comments!: any[];

    private postgres: PostgresDriver;
    private comparator: Comparator;

    constructor(db: Client) {
        this.db = db;

        this.triggers = [];
        this.functions = [];

        this.postgres = new PostgresDriver(db);
        this.comparator = new Comparator();
    }

    async load() {
        this.triggers = [];
        this.functions = [];

        // TODO: any => type
        const comments: any[] = [];
        
        const functions = await this.postgres.loadFunctions();
        for (const func of functions) {

            this.functions.push(func);

            if ( func.comment ) {
                comments.push({
                    function: function2identifyJson(func),
                    comment: func.comment
                });
            }
        }

        const triggers = await this.postgres.loadTriggers();
        for (const trigger of triggers) {

            this.triggers.push(trigger);

            if ( trigger.comment ) {
                comments.push({
                    trigger: {
                        schema: trigger.table.schema,
                        table: trigger.table.name,
                        name: trigger.name
                    },
                    comment: trigger.comment
                });
            }
        }

        if ( comments.length ) {
            this.comments = comments;
        }
    }

    // compare filesState and dbState
    getDiff(filesState: {
        // TODO: any => type
        functions: any[];
        triggers: any[];
        comments: any[];
    }) {
        const diff = this.comparator.compare(this, filesState);
        return diff;
    }

    async unfreezeAll() {
        let ddlSql = "";

        const dbComments = this.comments || [];

        this.functions.forEach(func => {
            const comment = findCommentByFunction(dbComments, func);

            ddlSql += DbState.getUnfreezeFunctionSql( func, comment );
            ddlSql += ";";
        });

        this.triggers.forEach(trigger => {
            const comment = findCommentByTrigger(dbComments, trigger);

            ddlSql += DbState.getUnfreezeTriggerSql( trigger, comment );
            ddlSql += ";";
        });

        try {
            await this.db.query(ddlSql);
        } catch(err) {
            // redefine callstack
            const newErr = new Error(err.message);
            (newErr as any).originalError = err;
            
            throw newErr;
        }
    }

    toJSON() {
        return {
            functions: this.functions,
            triggers: this.triggers
        };
    }

    // TODO: any => type
    // sql code, which raise error on freeze function
    static getCheckFreezeFunctionSql(func: any, errorText: any, actionOnFreeze = "error") {
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

    // TODO: any => type
    static getCheckFreezeTriggerSql(trigger: any, errorText: string) {
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

    // TODO: any => type
    static getUnfreezeFunctionSql(func: any, comment: any) {
        let prefix = "";
        if ( comment ) {
            if ( typeof comment.comment === "string" ) {
                prefix = comment.comment + "\n";
            }
            else if ( comment.comment.content && typeof comment.comment.content === "string" ) {
                prefix = comment.comment.content + "\n";
            }
        }

        const funcIdentifySql = function2identifySql( func );
        return `
            comment on function ${ funcIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
        `;
    }

    // TODO: any => type
    static getUnfreezeTriggerSql(trigger: any, comment: any) {
        let prefix = "";
        if ( comment ) {
            if ( typeof comment.comment === "string" ) {
                prefix = comment.comment + "\n";
            }
            else if ( comment.comment.content && typeof comment.comment.content === "string" ) {
                prefix = comment.comment.content + "\n";
            }
        }

        const triggerIdentifySql = trigger2identifySql( trigger );
        return `
            comment on trigger ${ triggerIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
        `;
    }
}
