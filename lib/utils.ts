import pg from "pg";
import _ from "lodash";
import fs from "fs";
import {
    GrapeQLCoach,
    CreateFunction,
    CreateTrigger,
    Comment
} from "grapeql-lang";

const defaultConfig = {
    config: "ddl-manager-config",
    folder: "ddl",
    database: false,
    user: false,
    password: false,
    host: "localhost",
    port: 5432,
    unfreeze: false
};

// TODO: any => type
export function parseConfigFromArgs(args: any) {
    const config = _.clone(defaultConfig) as any;

    const configPath = process.cwd() + "/" + config.config;
    let fileConfig;
    try {
        fileConfig = require(configPath);
    } catch(err) {
        if ( !fs.existsSync(configPath) ) {
            throw new Error(`config file "${ configPath }" not found`);
        }
    }
    
    for (const key in fileConfig) {
        config[ key ] = fileConfig[ key ];
    }
    

    


    const dbConfig = parseDbConfig(config);
    for (const key in dbConfig) {
        config[ key ] = dbConfig[ key ];
    }

    if ( !dbConfig.database || !dbConfig.user || !dbConfig.password ) {
        throw new Error("config must contains options: database, user, password");
    }


    if ( "unfreeze" in args ) {
        const unfreezeStr = (args.unfreeze + "").toLowerCase();

        if ( unfreezeStr === "true" || unfreezeStr === "1" ) {
            config.unfreeze = true;
        }
        else if ( unfreezeStr === "false" || unfreezeStr === "0" ) {
            config.unfreeze = false;
        }
        else {
            throw new Error("invalid unfreeze option value: " + unfreezeStr);
        }
    }


    const folderPath = process.cwd() + "/" + config.folder;
    if ( !fs.existsSync(folderPath) ) {
        throw new Error(`folder "${ folderPath }" not found`);
    }

    return config;
}

// TODO: any => type
function parseDbConfig(args: any) {
    const config = _.clone(defaultConfig) as any;

    if ( "database" in args ) {
        config.database = args.database;
    }
    if ( "user" in args ) {
        config.user = args.user;
    }
    if ( "password" in args ) {
        config.password = args.password;
    }
    if ( "host" in args ) {
        config.host = args.host;
    }
    if ( "port" in args ) {
        config.port = args.port;
    }

    return config;
}

// TODO: any => type
export async function getDbClient(dbConfig: any) {
    const config = parseDbConfig(dbConfig);

    const dbClient = new pg.Client(config);
    await dbClient.connect();

    return dbClient;
}

// TODO: any => type
export function isDbClient(dbOrConfig: any) {
    return (
        dbOrConfig && 
        _.isFunction(dbOrConfig.query)
    );
}

// TODO: any => type
export function logDiff(diff: any) {
    diff.drop.triggers.forEach((trigger: any) => {
        const triggerIdentifySql = trigger2identifySql( trigger );
        // tslint:disable-next-line: no-console
        console.log("drop trigger " + triggerIdentifySql);
    });

    diff.drop.functions.forEach((func: any) => {
        const funcIdentifySql = function2identifySql( func );
        // tslint:disable-next-line: no-console
        console.log("drop function " + funcIdentifySql);
    });
    
    diff.create.functions.forEach((func: any) => {
        const funcIdentifySql = function2identifySql( func );
        // tslint:disable-next-line: no-console
        console.log("create function " + funcIdentifySql);
    });

    diff.create.triggers.forEach((trigger: any) => {
        const triggerIdentifySql = trigger2identifySql( trigger );
        // tslint:disable-next-line: no-console
        console.log("create trigger " + triggerIdentifySql);
    });
}

// TODO: any => type
export function findCommentByFunction(comments: any[], func: any) {
    return comments.find(comment => {
        if ( !comment.function ) {
            return;
        }

        const {schema, name, args} = comment.function;
        const identify = `${schema}.${name}(${ args.join(", ") })`;

        const funcIdentifySql = function2identifySql(func);
    
        return identify === funcIdentifySql;
    });
}

// TODO: any => type
export function findFunctionByComment(functions: any[], comment: any) {
    if ( !comment.function ) {
        return;
    }

    return functions.find(func => {
        const {schema, name, args} = comment.function;
        const identify = `${schema}.${name}(${ args.join(", ") })`;

        const funcIdentifySql = function2identifySql(func);
    
        return identify === funcIdentifySql;
    });
}

// TODO: any => type
export function findCommentByTrigger(comments: any[], trigger: any) {
    return comments.find(comment => {
        if ( !comment.trigger ) {
            return;
        }

        const {schema, table, name} = comment.trigger;
        const identify = `${name} on ${schema}.${table}`;
        
        const triggerIdentifySql = trigger2identifySql(trigger);

        return identify == triggerIdentifySql;
    });
}

// TODO: any => type
export function findTriggerByComment(triggers: any[], comment: any) {
    if ( !comment.trigger ) {
        return;
    }

    return triggers.find(trigger => {
        const {schema, table, name} = comment.trigger;
        const identify = `${name} on ${schema}.${table}`;
        
        const triggerIdentifySql = trigger2identifySql(trigger);

        return identify === triggerIdentifySql;
    });
}

export function wrapText(text: string) {
    text += "";
    let tag = "tag";
    let index = 1;
    while ( text.indexOf("$tag" + index + "$") !== -1 ) {
        index++;
    }
    tag += index;

    return `$${tag}$${ text }$${tag}$`;
}


// TODO: any => type
// public.some_func(bigint, text)
export function function2identifySql(func: any) {
    if ( typeof func.toJSON === "function" ) {
        func = func.toJSON();
    }

    const identify = function2identifyJson( func );

    const argsSql = identify.args.join(", ");
    return `${ identify.schema }.${ identify.name }(${ argsSql })`;
}

// TODO: any => type
export function function2identifyJson(func: any) {
    if ( typeof func.toJSON === "function" ) {
        func = func.toJSON();
    }

    let args = func.args.filter((arg: any) => 
        !arg.out
    );

    args = args.map((arg: any) => 
        arg.type
    );
    
    return {
        schema: func.schema,
        name: func.name,
        args
    };
}

// TODO: any => type
export function function2dropSql(func: any) {
    // public.some_func(bigint, text)
    const identifySql = function2identifySql(func);

    return `drop function if exists ${ identifySql }`;
}

// TODO: any => type
export function trigger2sql(trigger: any) {
    if ( typeof trigger.toJSON === "function" ) {
        trigger = trigger.toJSON();
    }

    let out = "create ";

    if ( trigger.constraint ) {
        out += "constraint ";
    }
    
    out += `trigger ${trigger.name}\n`;

    // after|before
    if ( trigger.before ) {
        out += "before";
    }
    else if ( trigger.after ) {
        out += "after";
    }
    out += " ";

    // insert or update of x or delete
    const events = [];
    if ( trigger.insert ) {
        events.push("insert");
    }
    if ( trigger.update ) {
        if ( trigger.updateOf && trigger.updateOf.length ) {
            events.push(`update of ${ trigger.updateOf.join(", ") }`);
        }
        else if ( trigger.update === true ) {
            events.push("update");
        }
    }
    if ( trigger.delete ) {
        events.push("delete");
    }
    out += events.join(" or ");


    // table
    out += "\non ";
    out += `${trigger.table.schema}.${trigger.table.name}`;

    if ( trigger.notDeferrable ) {
        out += " not deferrable";
    }
    if ( trigger.deferrable ) {
        out += " deferrable";
    }
    if ( trigger.initially ) {
        out += " initially ";
        out += trigger.initially;
    }


    if ( trigger.statement ) {
        out += "\nfor each statement ";
    } else {
        out += "\nfor each row ";
    }

    if ( trigger.when ) {
        out += "\nwhen ( ";
        out += trigger.when;
        out += " ) ";
    }

    out += `\nexecute procedure ${trigger.procedure.schema}.${trigger.procedure.name}()`;

    return out;
}

// TODO: any => type
export function trigger2dropSql(trigger: any) {
    const identifySql = trigger2identifySql(trigger);
    return `drop trigger if exists ${ identifySql }`;
}

// TODO: any => type
// some_trigger on public.test
export function trigger2identifySql(trigger: any) {
    if ( typeof trigger.toJSON === "function" ) {
        trigger = trigger.toJSON();
    }

    let triggerTable = trigger.table;
    if ( triggerTable.row ) {
        triggerTable = triggerTable.row;
    }

    return `${trigger.name} on ${ triggerTable.schema }.${ triggerTable.name }`;
}

// TODO: any => type
export function comment2sql(comment: any) {
    if ( comment.row ) {
        comment = comment.row;
    }

    let commentContent = comment.comment;
    // if commentContent is PgString
    if ( commentContent.content ) {
        commentContent = commentContent.content;
    }

    if ( comment.function ) {
        const {schema, name, args} = comment.function;
        return `comment on function ${schema}.${name}(${ args.join(", ") }) is ${ wrapText(commentContent) }`;
    }
    else {
        const {name, schema, table} = comment.trigger;
        return `comment on trigger ${name} on ${schema}.${table} is ${ wrapText(commentContent) }`;
    }
}

// TODO: any => type
export function comment2dropSql(comment: any) {
    if ( comment.row ) {
        comment = comment.row;
    }

    if ( comment.function ) {
        const {schema, name, args} = comment.function;
        return `comment on function ${schema}.${name}(${ args.join(", ") }) is null`;
    }
    else {
        const {name, schema, table} = comment.trigger;
        return `comment on trigger ${name} on ${schema}.${table} is null`;
    }
}

export function replaceComments(sql: string) {
    const coach = new GrapeQLCoach(sql);

    const startIndex = coach.i;
    const newStr = coach.str.split("");

    for (; coach.i < coach.n; coach.i++) {
        const i = coach.i;

        // ignore comments inside function
        if ( coach.is(CreateFunction) ) {
            coach.parse(CreateFunction);
            coach.i--;
            continue;
        }

        if ( coach.is(CreateTrigger) ) {
            coach.parse(CreateTrigger);
            coach.i--;
            continue;
        }

        if ( coach.is(Comment) ) {
            coach.parse(Comment);

            const length = coach.i - i;
            // safe \n\r
            const spaceStr = coach.str.slice(i, i + length).replace(/[^\n\r]/g, " ");

            newStr.splice(i, length, ...spaceStr.split("") );
            
            coach.i--;
            continue;
        }
    }

    coach.i = startIndex;
    coach.str = newStr.join("");

    return coach;
}

// TODO: any => type
export function function2sql(func: any) {
    let additionalParams = "";

    additionalParams += " language ";
    additionalParams += func.language;
    
    if ( func.immutable ) {
        additionalParams += " immutable";
    }
    else if ( func.stable ) {
        additionalParams += " stable";
    }

    if ( func.returnsNullOnNull ) {
        additionalParams += " returns null on null input";
    }
    else if ( func.strict ) {
        additionalParams += " strict";
    }


    if ( func.parallel ) {
        additionalParams += " parallel ";
        additionalParams += func.parallel;
    }

    if ( func.cost != null ) {
        additionalParams += " cost " + func.cost;
    }

    
    const returnsSql = returns2sql(func.returns);

    let argsSql = func.args.map((arg: any) => 
        "    " + arg2sql(arg)
    ).join(",\n");

    if ( func.args.length ) {
        argsSql = "\n" + argsSql + "\n";
    }


    let body = func.body;
    if ( typeof body.content === "string" ) {
        body = `$body$${ body.content }$body$`;
    }
    else if ( typeof body === "string" ) {
        body = `$body$${ body }$body$`;
    }
    else {
        body = body.toString();
    }

    // отступов не должно быть!
    // иначе DDLManager.dump будет писать некрасивый код
    return `
create or replace function ${ func.schema }.${ func.name }(${argsSql}) 
returns ${ returnsSql } 
${ additionalParams }
as ${ body }
    `.trim();
}

// TODO: any => type
function returns2sql(returns: any) {
    let out = "";

    if ( returns.setof ) {
        out += "setof ";
    }

    if ( returns.table ) {
        out += `table(${ 
            returns.table.map((arg: any) => 
                arg2sql(arg)
            ).join(", ") 
        })`;
    } else {
        out += returns.type;
    }

    return out;
}

// TODO: any => type
function arg2sql(arg: any) {
    let out = "";

    if ( arg.out ) {
        out += "out ";
    }
    else if ( arg.in ) {
        out += "in ";
    }

    if ( arg.name ) {
        out += arg.name;
        out += " ";
    }

    out += arg.type;

    if ( arg.default ) {
        out += " default ";
        out += arg.default;
    }

    return out;
}
