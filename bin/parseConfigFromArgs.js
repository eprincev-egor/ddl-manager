"use strict";

const _ = require("lodash");
const fs = require("fs");

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

module.exports = {parseConfigFromArgs};