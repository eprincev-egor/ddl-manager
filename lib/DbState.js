"use strict";

const DDLCoach = require("./parser/DDLCoach");
const CreateFunction = require("./parser/syntax/CreateFunction");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const _ = require("lodash");
const {
    findCommentByFunction,
    findCommentByTrigger,
    wrapText
} = require("./utils");

class DbState {
    constructor(db) {
        this.db = db;

        this.triggers = [];
        this.functions = [];
    }

    async load() {
        let db = this.db;
        let sql;

        let result;
        this.triggers = [];
        this.functions = [];
        let comments = [];
        
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
            let newErr = new Error(err.message);
            newErr.originalError = err;
            throw newErr;
        }
        

        result.rows.forEach(row => {
            let {ddl} = row;

            let coach = new DDLCoach(ddl);
            let func = coach.parseCreateFunction();
            let json = func.toJSON();

            // function was created by ddl manager
            let canSyncFunction = (
                row.comment &&
                /ddl-manager-sync$/i.test(row.comment)
            );
            json.freeze = !canSyncFunction;

            if ( row.comment ) {
                let comment = row.comment.replace(/ddl-manager-sync$/i, "");
                comment = comment.trim();

                if ( comment ) {
                    comments.push({
                        function: CreateFunction.function2identifyJson(json),
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
            let newErr = new Error(err.message);
            newErr.originalError = err;
            throw newErr;
        }
        

        result.rows.forEach(row => {
            let {ddl} = row;

            let coach = new DDLCoach(ddl);
            let trigger = coach.parseCreateTrigger();
            let json = trigger.toJSON();

            // trigger was created by ddl manager
            let canSyncTrigger = (
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
    getDiff(filesState) {
        if ( !_.isObject(filesState) ) {
            throw new Error("undefined filesState");
        }
        if ( !_.isArray(filesState.functions) ) {
            throw new Error("undefined filesState.functions");
        }
        if ( !_.isArray(filesState.triggers) ) {
            throw new Error("undefined filesState.triggers");
        }

        
        let drop = {
            functions: [],
            triggers: []
        };
        let create = {
            functions: [],
            triggers: []
        };

        
        for (let i = 0, n = this.functions.length; i < n; i++) {
            let func = this.functions[ i ];
            
            // ddl-manager cannot drop freeze function
            if ( func.freeze ) {
                continue;
            }

            let existsSameFuncFromFile = filesState.functions.some(fileFunc =>
                equalFunction(fileFunc, func)
            );

            if ( existsSameFuncFromFile ) {
                continue;
            }

            // for drop function, need drop trigger, who call it function
            if ( func.returns.type == "trigger" ) {
                let depsTriggers = this.triggers.filter(dbTrigger => {
                    let isDepsTrigger = (
                        dbTrigger.procedure.schema == func.schema &&
                        dbTrigger.procedure.name == func.name
                    );

                    if ( !isDepsTrigger ) {
                        return false;
                    }
                    
                    let existsSameTriggerFromFile = filesState.triggers.some(fileTrigger =>
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
            let trigger = this.triggers[ i ];

            // ddl-manager cannot drop freeze function
            if ( trigger.freeze ) {
                continue;
            }

            let existsSameTriggerFromFile = filesState.triggers.some(fileTrigger =>
                equalTrigger(fileTrigger, trigger)
            );

            if ( existsSameTriggerFromFile ) {
                continue;
            }

            drop.triggers.push( trigger );
        }

        let dbComments = this.comments || [];
        for (let i = 0, n = dbComments.length; i < n; i++) {
            let comment = dbComments[ i ];
            let fileComments = filesState.comments || [];

            let existsSameCommentFromFile = fileComments.some(fileComment =>
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
            let func = filesState.functions[ i ];

            let existsSameFuncFromDb = this.functions.find(dbFunc =>
                equalFunction(dbFunc, func)
            );

            if ( existsSameFuncFromDb ) {
                continue;
            }

            create.functions.push(func);
        }
        for (let i = 0, n = filesState.triggers.length; i < n; i++) {
            let trigger = filesState.triggers[ i ];
            
            let existsSameTriggerFromDb = this.triggers.some(dbTrigger =>
                equalTrigger(dbTrigger, trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            create.triggers.push( trigger );
        }

        let fileComments = filesState.comments || [];
        for (let i = 0, n = fileComments.length; i < n; i++) {
            let comment = fileComments[ i ];
            
            let existsSameCommentFromDb = dbComments.some(dbComment =>
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

        let dbComments = this.comments || [];

        this.functions.forEach(func => {
            let comment = findCommentByFunction(dbComments, func);

            ddlSql += DbState.getUnfreezeFunctionSql( func, comment );
            ddlSql += ";";
        });

        this.triggers.forEach(trigger => {
            let comment = findCommentByTrigger(dbComments, trigger);

            ddlSql += DbState.getUnfreezeTriggerSql( trigger, comment );
            ddlSql += ";";
        });

        try {
            await this.db.query(ddlSql);
        } catch(err) {
            // redefine callstack
            let newErr = new Error(err.message);
            newErr.originalError = err;
            
            throw newErr;
        }
    }

    toJSON() {
        return {
            functions: this.functions,
            triggers: this.triggers
        };
    }

    // sql code, which raise error on freeze function
    static getCheckFreezeFunctionSql(func, errorText) {
        let funcIdentifySql = CreateFunction.function2identifySql( func );

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
                        raise exception ${wrapText(errorText)};
                    end if;
                end
            $$;
        `;
    }

    static getCheckFreezeTriggerSql(trigger, errorText) {
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

    static getUnfreezeFunctionSql(func, comment) {
        let prefix = "";
        if ( comment ) {
            prefix = comment.comment + "\n";
        }

        let funcIdentifySql = CreateFunction.function2identifySql( func );
        return `
            comment on function ${ funcIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
        `;
    }

    static getUnfreezeTriggerSql(trigger, comment) {
        let prefix = "";
        if ( comment ) {
            prefix = comment.comment + "\n";
        }

        let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
        return `
            comment on trigger ${ triggerIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
        `;
    }
}

function equalFunction(func1, func2) {
    return (
        func1.schema == func2.schema &&
        func1.name == func2.name &&
        func1.body == func2.body &&
        func1.language == func2.language &&
        equalFunctionReturns(
            func1.returns,
            func2.returns
        ) &&
        equalFunctionArguments(
            func1.args,
            func2.args
        )
    );
}

function equalFunctionReturns(returns1, returns2) {
    if ( returns1.setof != returns2.setof ) {
        return false;
    }

    if ( returns1.type && returns2.type ) {
        return returns1.type == returns2.type;
    }
    else if ( returns1.table && returns2.table ) {
        return equalFunctionArguments(
            returns1.table,
            returns2.table
        );
    }
    else {
        return false;
    }
}

function equalFunctionArguments(args1, args2) {
    if ( args1.length != args2.length ) {
        return false;
    }

    return args1.every((arg1, i) => 
        equalArgument(
            arg1,
            args2[ i ]
        )
    );
}

function equalArgument(arg1, arg2) {
    return (
        arg1.name == arg2.name &&
        arg1.type == arg2.type &&
        arg1.in == arg2.in &&
        arg1.out == arg2.out 
    );
}

function equalTrigger(trigger1, trigger2) {
    return (
        trigger1.name == trigger2.name &&

        trigger1.table && trigger2.table &&
        trigger1.table.schema == trigger2.table.schema &&
        trigger1.table.name == trigger2.table.name &&
        
        trigger1.procedure && trigger2.procedure &&
        trigger1.procedure.schema == trigger2.procedure.schema &&
        trigger1.procedure.name == trigger2.procedure.name &&

        equalTriggerEvent(
            trigger1,
            trigger2
        )
    );
}

function equalTriggerEvent(trigger1, trigger2) {
    if ( trigger1.before != trigger2.before ) {
        return false;
    }
    if ( trigger1.after != trigger2.after ) {
        return false;
    }
    if ( trigger1.insert != trigger2.insert ) {
        return false;
    }
    if ( trigger1.delete != trigger2.delete ) {
        return false;
    }

    // both without update
    if ( !trigger1.update && !trigger2.update ) {
        return true;
    }

    // or both update === true
    if ( trigger1.update === true && trigger2.update === true ) {
        return true;
    }

    // if some has update, but another hasn't
    if ( 
        !trigger1.update && trigger2.update 
        ||
        trigger1.update && !trigger2.update 
    ) {
        return false;
    }

    // if some is array, but another not
    if (  
        !_.isArray(trigger1.update) && _.isArray(trigger1.update)
        ||
        _.isArray(trigger1.update) && !_.isArray(trigger1.update)
    ) {
        return false;
    }


    // now both is array
    if ( trigger1.update.length != trigger2.update.length ) {
        return false;
    }

    return trigger1.update.every((name1, i) => 
        name1 == trigger2.update[ i ]
    );
}

function equalComment(comment1, comment2) {
    if ( comment1.comment != comment2.comment ) {
        return false;
    }

    if ( comment1.function && comment2.function ) {
        return (
            comment1.function.schema == comment2.function.schema &&
            comment1.function.name == comment2.function.name &&

            comment1.function.args && comment2.function.args &&
            comment1.function.args.length == comment2.function.args.length &&
            comment1.function.args.every((argType1, i) =>
                argType1 == comment2.function.args[i]
            )
        );
    }
    else if ( comment1.trigger && comment2.trigger ) {
        return (
            comment1.trigger.schema == comment2.trigger.schema &&
            comment1.trigger.name == comment2.trigger.name &&
            comment1.trigger.table == comment2.trigger.table
        );
    }
    else {
        return false;
    }
}

module.exports = DbState;