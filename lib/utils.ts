import pg from "pg";
import _ from "lodash";
import fs from "fs";
import { IDiff } from "./interface";

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
