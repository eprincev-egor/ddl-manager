"use strict";

const pg = require("pg");
const _ = require("lodash");
const fs = require("fs");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const CreateFunction = require("./parser/syntax/CreateFunction");

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
        let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
        console.log("drop trigger " + triggerIdentifySql);
    });

    diff.drop.functions.forEach(func => {
        let funcIdentifySql = CreateFunction.function2identifySql( func );
        console.log("drop function " + funcIdentifySql);
    });
    
    diff.create.functions.forEach(func => {
        let funcIdentifySql = CreateFunction.function2identifySql( func );
        console.log("create function " + funcIdentifySql);
    });

    diff.create.triggers.forEach(trigger => {
        let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
        console.log("create trigger " + triggerIdentifySql);
    });
}

function findFunctionComment(comments, func) {
    return comments.find(comment => {
        if ( !comment.function ) {
            return;
        }

        let {schema, name, args} = comment.function;
        let identify = `${schema}.${name}(${ args.join(", ") })`;

        let funcIdentifySql = CreateFunction.function2identifySql(func);
    
        return identify == funcIdentifySql;
    });
}

function findTriggerComment(comments, trigger) {
    return comments.find(comment => {
        if ( !comment.trigger ) {
            return;
        }

        let {schema, table, name} = comment.trigger;
        let identify = `${name} on ${schema}.${table}`;
        
        let triggerIdentifySql = CreateTrigger.trigger2identifySql(trigger);

        return identify == triggerIdentifySql;
    });
}

module.exports = {
    getDbClient,
    isDbClient,
    logDiff,
    parseConfigFromArgs,
    findFunctionComment,
    findTriggerComment
};