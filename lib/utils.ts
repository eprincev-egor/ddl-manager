import pg from "pg";
import _ from "lodash";
import fs from "fs";

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
        const triggerIdentifySql = trigger.getSignature();
        // tslint:disable-next-line: no-console
        console.log("drop trigger " + triggerIdentifySql);
    });

    diff.drop.functions.forEach((func: any) => {
        const funcIdentifySql = func.getSignature();
        // tslint:disable-next-line: no-console
        console.log("drop function " + funcIdentifySql);
    });
    
    diff.create.functions.forEach((func: any) => {
        const funcIdentifySql = func.getSignature();
        // tslint:disable-next-line: no-console
        console.log("create function " + funcIdentifySql);
    });

    diff.create.triggers.forEach((trigger: any) => {
        const triggerIdentifySql = trigger.getSignature();
        // tslint:disable-next-line: no-console
        console.log("create trigger " + triggerIdentifySql);
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
export function function2dropSql(func: any) {
    // public.some_func(bigint, text)
    const identifySql = func.getSignature();

    return `drop function if exists ${ identifySql }`;
}

// TODO: any => type
export function trigger2sql(trigger: any) {
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
    const identifySql = trigger.getSignature();
    return `drop trigger if exists ${ identifySql }`;
}

// TODO: any => type
export function comment2sql(
    comment: string,
    info: {trigger?: any; function?: any} = {}
) {

    if ( info.function ) {
        const identify = info.function.getSignature();
        return `comment on function ${identify} is ${ wrapText(comment) }`;
    }
    else {
        const identify = info.trigger.getSignature();
        return `comment on trigger ${identify} is ${ wrapText(comment) }`;
    }
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
