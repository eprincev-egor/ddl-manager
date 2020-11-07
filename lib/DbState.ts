import _ from "lodash";
import {
    findCommentByFunction,
    findCommentByTrigger,
    wrapText,

    function2identifySql,
    function2identifyJson,
    trigger2identifySql
} from "./utils";
import assert from "assert";
import { Client } from "pg";
import { FunctionParser } from "./parser/FunctionParser";
import { TriggerParser } from "./parser/TriggerParser";

export class DbState {
    private db: Client;
    // TODO: any => type
    triggers: any[];
    functions: any[];
    comments!: any[];

    private functionParser: FunctionParser;
    private triggerParser: TriggerParser;

    constructor(db: Client) {
        this.db = db;

        this.triggers = [];
        this.functions = [];

        this.functionParser = new FunctionParser();
        this.triggerParser = new TriggerParser();
    }

    async load() {
        const db = this.db;
        let sql;

        let result;
        this.triggers = [];
        this.functions = [];
        // TODO: any => type
        const comments: any[] = [];
        
        sql = `
            select
                pg_get_functiondef( pg_proc.oid ) as ddl,
                pg_catalog.obj_description( pg_proc.oid ) as comment
            from information_schema.routines as routines

            left join pg_catalog.pg_proc as pg_proc on
                routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
            
            -- ignore language C or JS
            inner join pg_catalog.pg_language as pg_language on
                pg_language.oid = pg_proc.prolang and
                lower(pg_language.lanname) in ('sql', 'plpgsql')
            
            where
                routines.routine_schema <> 'pg_catalog' and
                routines.routine_schema <> 'information_schema' and
                routines.routine_definition is distinct from 'aggregate_dummy' and
                not exists(
                    select from pg_catalog.pg_aggregate as pg_aggregate
                    where
                        pg_aggregate.aggtransfn = pg_proc.oid or
                        pg_aggregate.aggfinalfn = pg_proc.oid
                )
            
            order by
                routines.routine_schema, 
                routines.routine_name
        `;
        try { 
            result = await db.query(sql);
        } catch(err) {
            // redefine callstack
            const newErr = new Error(err.message);
            (newErr as any).originalError = err;
            throw newErr;
        }
        

        result.rows.forEach(row => {
            const {ddl} = row;

            const func = this.functionParser.parse(ddl);
            const json: ReturnType<typeof func.toJSON> & {freeze?: boolean} = func.toJSON();

            // function was created by ddl manager
            const canSyncFunction = (
                row.comment &&
                /ddl-manager-sync$/i.test(row.comment)
            );
            json.freeze = !canSyncFunction;

            if ( row.comment ) {
                let comment = row.comment.replace(/ddl-manager-sync$/i, "");
                comment = comment.trim();

                if ( comment ) {
                    comments.push({
                        function: function2identifyJson(json),
                        comment
                    });
                }
            }

            this.functions.push(json);
        });

        sql = `
            select
                pg_get_triggerdef( pg_trigger.oid ) as ddl,
                pg_catalog.obj_description( pg_trigger.oid ) as comment
            from pg_trigger
            where
                pg_trigger.tgisinternal = false
        `;
        try { 
            result = await db.query(sql);
        } catch(err) {
            // redefine callstack
            const newErr = new Error(err.message);
            (newErr as any).originalError = err;
            throw newErr;
        }
        

        result.rows.forEach(row => {
            const {ddl} = row;

            const trigger = this.triggerParser.parse(ddl);
            const json: ReturnType<typeof trigger.toJSON> & {
                freeze?: boolean;
                table: {
                    schema: string;
                    name: string;
                }
            } = trigger.toJSON() as any;

            // trigger was created by ddl manager
            const canSyncTrigger = (
                row.comment &&
                /ddl-manager-sync/i.test(row.comment)
            );
            json.freeze = !canSyncTrigger;

            if ( row.comment ) {
                let comment = row.comment.replace(/ddl-manager-sync$/i, "");
                comment = comment.trim();

                if ( comment ) {
                    comments.push({
                        trigger: {
                            schema: json.table.schema,
                            table: json.table.name,
                            name: json.name
                        },
                        comment
                    });
                }
            }

            this.triggers.push(json);
        });

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
        if ( !_.isObject(filesState) ) {
            throw new Error("undefined filesState");
        }
        if ( !_.isArray(filesState.functions) ) {
            throw new Error("undefined filesState.functions");
        }
        if ( !_.isArray(filesState.triggers) ) {
            throw new Error("undefined filesState.triggers");
        }

        // TODO: any => type
        const drop: {
            functions: any[];
            triggers: any[];
            comments?: any[];
        } = {
            functions: [],
            triggers: []
        };
        const create: {
            functions: any[];
            triggers: any[];
            comments?: any[];
        } = {
            functions: [],
            triggers: []
        };

        
        for (let i = 0, n = this.functions.length; i < n; i++) {
            const func = this.functions[ i ];
            
            // ddl-manager cannot drop freeze function
            if ( func.freeze ) {
                continue;
            }

            const existsSameFuncFromFile = filesState.functions.some(fileFunc =>
                equalFunction(fileFunc, func)
            );

            if ( existsSameFuncFromFile ) {
                continue;
            }

            // for drop function, need drop trigger, who call it function
            if ( func.returns.type === "trigger" ) {
                const depsTriggers = this.triggers.filter(dbTrigger => {
                    const isDepsTrigger = (
                        dbTrigger.procedure.schema === func.schema &&
                        dbTrigger.procedure.name === func.name
                    );

                    if ( !isDepsTrigger ) {
                        return false;
                    }
                    
                    const existsSameTriggerFromFile = filesState.triggers.some(fileTrigger =>
                        equalTrigger(fileTrigger, dbTrigger)
                    );

                    // if trigger has change, then he will dropped
                    // in next cycle
                    if ( !existsSameTriggerFromFile ) {
                        return false;
                    }

                    // we have trigger and he without changes
                    return true;
                });

                depsTriggers.forEach(fileTrigger => {
                    // drop
                    drop.triggers.push( fileTrigger );
                    // and create again
                    create.triggers.push( fileTrigger );
                });
            }
            
            
            drop.functions.push(func);
        }

        for (let i = 0, n = this.triggers.length; i < n; i++) {
            const trigger = this.triggers[ i ];

            // ddl-manager cannot drop freeze function
            if ( trigger.freeze ) {
                continue;
            }

            const existsSameTriggerFromFile = filesState.triggers.some(fileTrigger =>
                equalTrigger(fileTrigger, trigger)
            );

            if ( existsSameTriggerFromFile ) {
                continue;
            }

            drop.triggers.push( trigger );
        }

        const dbComments = this.comments || [];
        for (let i = 0, n = dbComments.length; i < n; i++) {
            const comment = dbComments[ i ];

            const existsSameCommentFromFile = (filesState.comments || []).some(fileComment =>
                equalComment(fileComment, comment)
            );

            if ( existsSameCommentFromFile ) {
                continue;
            }

            if ( !drop.comments ) {
                drop.comments = [];
            }
            drop.comments.push( comment );
        }



        for (let i = 0, n = filesState.functions.length; i < n; i++) {
            const func = filesState.functions[ i ];

            const existsSameFuncFromDb = this.functions.find(dbFunc =>
                equalFunction(dbFunc, func)
            );

            if ( existsSameFuncFromDb ) {
                continue;
            }

            create.functions.push(func);
        }
        for (let i = 0, n = filesState.triggers.length; i < n; i++) {
            const trigger = filesState.triggers[ i ];
            
            const existsSameTriggerFromDb = this.triggers.some(dbTrigger =>
                equalTrigger(dbTrigger, trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            create.triggers.push( trigger );
        }

        const fileComments = filesState.comments || [];
        for (let i = 0, n = fileComments.length; i < n; i++) {
            const comment = fileComments[ i ];
            
            const existsSameCommentFromDb = dbComments.some(dbComment =>
                equalComment(dbComment, comment)
            );

            if ( existsSameCommentFromDb ) {
                continue;
            }

            if ( !create.comments ) {
                create.comments = [];
            }
            create.comments.push( comment );
        }



        return {
            drop,
            create
        };
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

function equalFunction(func1: any, func2: any) {
    return deepEqual(func1, func2);
}

function equalTrigger(trigger1: any, trigger2: any) {
    return deepEqual(trigger1, trigger2);
}

function equalComment(comment1: any, comment2: any) {
    return deepEqual(comment1, comment2);
}

function deepEqual(obj1: any, obj2: any) {
    try {
        assert.deepStrictEqual(obj1, obj2);
        return true;
    } catch(err) {
        return false;
    }
}
