"use strict";

const pg = require("pg");
const _ = require("lodash");
const fs = require("fs");
const {
    GrapeQLCoach,
    CreateFunction,
    CreateTrigger,
    Comment,
    DataType,
    ObjectLink
} = require("grapeql-lang");

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

function parseConfigFromArgs(args) {
    let config = _.clone(defaultConfig);

    let configPath = process.cwd() + "/" + config.config;
    let fileConfig;
    try {
        fileConfig = require(configPath);
    } catch(err) {
        if ( !fs.existsSync(configPath) ) {
            throw new Error(`config file "${ configPath }" not found`);
        }
    }
    
    for (let key in fileConfig) {
        config[ key ] = fileConfig[ key ];
    }
    

    


    let dbConfig = parseDbConfig(config);
    for (let key in dbConfig) {
        config[ key ] = dbConfig[ key ];
    }

    if ( !dbConfig.database || !dbConfig.user || !dbConfig.password ) {
        throw new Error("config must contains options: database, user, password");
    }


    if ( "unfreeze" in args ) {
        let unfreeze = args.unfreeze + "";
        unfreeze = unfreeze.toLowerCase();

        if ( unfreeze == "true" || unfreeze == "1" ) {
            unfreeze = true;
        }
        else if ( unfreeze == "false" || unfreeze == "0" ) {
            unfreeze = false;
        }
        else {
            throw new Error("invalid unfreeze option value: " + unfreeze);
        }

        config.unfreeze = unfreeze;
    }


    let folderPath = process.cwd() + "/" + config.folder;
    if ( !fs.existsSync(folderPath) ) {
        throw new Error(`folder "${ folderPath }" not found`);
    }

    return config;
}

function parseDbConfig(args) {
    let config = _.clone(defaultConfig);

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

async function getDbClient(dbConfig) {
    let config = parseDbConfig(dbConfig);

    let dbClient = new pg.Client(config);
    await dbClient.connect();

    return dbClient;
}

function isDbClient(dbOrConfig) {
    return (
        dbOrConfig && 
        _.isFunction(dbOrConfig.query)
    );
}

function logDiff(diff) {
    diff.drop.triggers.forEach(trigger => {
        let triggerIdentifySql = trigger2identifySql( trigger );
        console.log("drop trigger " + triggerIdentifySql);
    });

    diff.drop.functions.forEach(func => {
        let funcIdentifySql = function2identifySql( func );
        console.log("drop function " + funcIdentifySql);
    });
    
    diff.create.functions.forEach(func => {
        let funcIdentifySql = function2identifySql( func );
        console.log("create function " + funcIdentifySql);
    });

    diff.create.triggers.forEach(trigger => {
        let triggerIdentifySql = trigger2identifySql( trigger );
        console.log("create trigger " + triggerIdentifySql);
    });
}

function findCommentByFunction(comments, func) {
    return comments.find(comment => {
        if ( !comment.function ) {
            return;
        }

        let {schema, name, args} = comment.function;
        let identify = `${schema}.${name}(${ args.join(", ") })`;

        let funcIdentifySql = function2identifySql(func);
    
        return identify == funcIdentifySql;
    });
}

function findFunctionByComment(functions, comment) {
    if ( !comment.function ) {
        return;
    }

    return functions.find(func => {
        let {schema, name, args} = comment.function;
        let identify = `${schema}.${name}(${ args.join(", ") })`;

        let funcIdentifySql = function2identifySql(func);
    
        return identify == funcIdentifySql;
    });
}

function findCommentByTrigger(comments, trigger) {
    return comments.find(comment => {
        if ( !comment.trigger ) {
            return;
        }

        let {schema, table, name} = comment.trigger;
        let identify = `${name} on ${schema}.${table}`;
        
        let triggerIdentifySql = trigger2identifySql(trigger);

        return identify == triggerIdentifySql;
    });
}

function findTriggerByComment(triggers, comment) {
    if ( !comment.trigger ) {
        return;
    }

    return triggers.find(trigger => {
        let {schema, table, name} = comment.trigger;
        let identify = `${name} on ${schema}.${table}`;
        
        let triggerIdentifySql = trigger2identifySql(trigger);

        return identify == triggerIdentifySql;
    });
}

function wrapText(text) {
    text += "";
    let tag = "tag";
    let index = 1;
    while ( text.indexOf("$tag" + index + "$") != -1 ) {
        index++;
    }
    tag += index;

    return `$${tag}$${ text }$${tag}$`;
}


// public.some_func(bigint, text)
function function2identifySql(func) {
    if ( func.row ) {
        func = func.row;
    }

    let identify = function2identifyJson( func );

    let argsSql = identify.args.join(", ");
    return `${ identify.schema }.${ identify.name }(${ argsSql })`;
}

function function2identifyJson(func) {
    if ( func.row ) {
        func = func.row;
    }

    let args = func.args.filter(arg => 
        !arg.out
    );

    args = args.map(arg => 
        arg.type
    );
    
    return {
        schema: func.schema,
        name: func.name,
        args
    };
}

function function2dropSql(func) {
    // public.some_func(bigint, text)
    let identifySql = function2identifySql(func);

    return `drop function if exists ${ identifySql }`;
}

function parseFunctionIdentify(coach) {
    let {schema, name} = parseSchemaName(coach);

    coach.skipSpace();
    coach.expect("(");
    coach.skipSpace();

    let args = coach.parseComma(DataType);
    args = args.map(arg =>
        arg.type
    );

    coach.skipSpace();
    coach.expect(")");

    return {
        schema,
        name,
        args
    };
}

function parseSchemaName(coach) {
    // name
    let i = coach.i;
    let objectLink = coach.parse(ObjectLink);
    if ( 
        objectLink.row.link.length != 2 &&
        objectLink.row.link.length != 1
    ) {
        coach.i = i;
        coach.throwError("invalid name " + objectLink.toString());
    }

    let schema = "public";
    let name = objectLink.row.link[0].toLowerCase();
    if ( objectLink.row.link.length == 2 ) {
        schema = name;
        name = objectLink.row.link[1].toLowerCase();
    }
    
    return {schema, name};
}

function trigger2sql(trigger) {
    if ( trigger.row ) {
        trigger = trigger.row;
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
    let events = [];
    if ( trigger.insert ) {
        events.push("insert");
    }
    if ( trigger.update ) {
        if ( trigger.update === true ) {
            events.push("update");
        } else {
            events.push(`update of ${ trigger.update.join(", ") }`);
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

function trigger2dropSql(trigger) {
    let identifySql = trigger2identifySql(trigger);
    return `drop trigger if exists ${ identifySql }`;
}

// some_trigger on public.test
function trigger2identifySql(trigger) {
    if ( trigger.row ) {
        trigger = trigger.row;
    }
    return `${trigger.name} on ${ trigger.table.schema }.${ trigger.table.name }`;
}

function comment2sql(comment) {
    if ( comment.row ) {
        comment = comment.row;
    }

    let commentContent = comment.comment;
    // if commentContent is PgString
    if ( commentContent.content ) {
        commentContent = commentContent.content;
    }

    if ( comment.function ) {
        let {schema, name, args} = comment.function;
        return `comment on function ${schema}.${name}(${ args.join(", ") }) is ${ wrapText(commentContent) }`;
    }
    else {
        let {name, schema, table} = comment.trigger;
        return `comment on trigger ${name} on ${schema}.${table} is ${ wrapText(commentContent) }`;
    }
}

function comment2dropSql(comment) {
    if ( comment.row ) {
        comment = comment.row;
    }

    if ( comment.function ) {
        let {schema, name, args} = comment.function;
        return `comment on function ${schema}.${name}(${ args.join(", ") }) is null`;
    }
    else {
        let {name, schema, table} = comment.trigger;
        return `comment on trigger ${name} on ${schema}.${table} is null`;
    }
}

function replaceComments(sql) {
    const coach = new GrapeQLCoach(sql);

    let startIndex = coach.i;
    let newStr = coach.str.split("");

    for (; coach.i < coach.n; coach.i++) {
        let i = coach.i;

        // ignore comments inside function
        if ( coach.is(CreateFunction) ) {
            coach.parse(CreateFunction);
        }

        if ( coach.is(CreateTrigger) ) {
            coach.parse(CreateTrigger);
        }

        if ( coach.is(Comment) ) {
            coach.parse(Comment);

            let length = coach.i - i;
            // safe \n\r
            let spaceStr = coach.str.slice(i, i + length).replace(/[^\n\r]/g, " ");

            newStr.splice.apply(newStr, [i, length].concat( spaceStr.split("") ));
        }
    }

    coach.i = startIndex;
    coach.str = newStr.join("");

    return coach;
}

function function2sql(func) {
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

    
    let returnsSql = returns2sql(func.returns, {
        lineBreak: true
    });

    let argsSql = func.args.map(arg => 
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

function returns2sql(returns) {
    let out = "";

    if ( returns.setof ) {
        out += "setof ";
    }

    if ( returns.table ) {
        out += `table(${ 
            returns.table.map(arg => 
                arg2sql(arg)
            ).join(", ") 
        })`;
    } else {
        out += returns.type;
    }

    return out;
}

function arg2sql(arg) {
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

module.exports = {
    getDbClient,
    isDbClient,
    logDiff,
    parseConfigFromArgs,
    findCommentByFunction,
    findCommentByTrigger,
    findTriggerByComment,
    findFunctionByComment,
    wrapText,

    parseFunctionIdentify,
    function2dropSql,
    function2identifyJson,
    function2identifySql,
    trigger2sql,
    trigger2dropSql,
    trigger2identifySql,
    comment2sql,
    comment2dropSql,
    function2sql,

    replaceComments
};