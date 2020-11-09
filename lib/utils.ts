import pg from "pg";
import _ from "lodash";
import fs from "fs";
import { IDiff } from "./interface";
import { DatabaseTrigger } from "./ast/DatabaseTrigger";
import { DatabaseFunction } from "./ast/DatabaseFunction";
import assert from "assert";

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

export function logDiff(diff: IDiff) {
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

export function trigger2sql(trigger: DatabaseTrigger) {
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
    const events: string[] = [];
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

export function triggerCommentsSQL(trigger: DatabaseTrigger) {
    assert.ok(trigger.comment);
    return `comment on trigger ${trigger.getSignature()} is ${ wrapText(trigger.comment) }`;
}

export function functionCommentsSQL(func: DatabaseFunction) {
    assert.ok(func.comment);
    return `comment on function ${func.getSignature()} is ${ wrapText(func.comment) }`;
}
