"use strict";

const DbState = require("./DbState");
const FilesState = require("./FilesState");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const CreateFunction = require("./parser/syntax/CreateFunction");
const _ = require("lodash");
const pg = require("pg");

const watchers = [];

class DdlManager {
    static async migrate(db, diff) {
        if ( diff == null ) {
            throw new Error("invalid diff");
        }
        let out = {
            errors: []
        };

        // drop old objects
        for (let i = 0, n = diff.drop.triggers.length; i < n; i++) {
            let ddlSql = "";
            let trigger = diff.drop.triggers[i];
            let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
            
            // check freeze object
            let checkFreezeSql = DbState.getCheckFreezeTriggerSql( 
                trigger,
                `cannot drop freeze trigger ${ triggerIdentifySql }`
            );
            ddlSql = checkFreezeSql;

            ddlSql += ";";
            ddlSql += CreateTrigger.trigger2dropSql(trigger);

            try {
                await db.query(ddlSql);
            } catch(err) {
                out.errors.push(err);
            }
        }
        
        for (let i = 0, n = diff.drop.functions.length; i < n; i++) {
            let ddlSql = "";
            let func = diff.drop.functions[i];
            let funcIdentifySql = CreateFunction.function2identifySql( func );

            // check freeze object
            let checkFreezeSql = DbState.getCheckFreezeFunctionSql( 
                func,
                `cannot drop freeze function ${ funcIdentifySql }`
            );
            
            ddlSql = checkFreezeSql;

            ddlSql += ";";
            ddlSql += CreateFunction.function2dropSql(func);

            try {
                await db.query(ddlSql);
            } catch(err) {
                out.errors.push(err);
            }
        }

        // create new objects
        for (let i = 0, n = diff.create.functions.length; i < n; i++) {
            let ddlSql = "";
            let func = diff.create.functions[i];
            let funcIdentifySql = CreateFunction.function2identifySql( func );

            // check freeze object
            let checkFreezeSql = DbState.getCheckFreezeFunctionSql( 
                func,
                `cannot replace freeze function ${ funcIdentifySql }`
            );
            
            ddlSql = checkFreezeSql;


            ddlSql += ";";
            ddlSql += CreateFunction.function2sql( func );
            
            ddlSql += ";";
            ddlSql += `
                comment on function ${ funcIdentifySql } is 'ddl-manager-sync'
            `;

            try {
                await db.query(ddlSql);
            } catch(err) {
                out.errors.push(err);
            }
        }

        for (let i = 0, n = diff.create.triggers.length; i < n; i++) {
            let ddlSql = "";
            let trigger = diff.create.triggers[i];
            let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
            
            // check freeze object
            let checkFreezeSql = DbState.getCheckFreezeTriggerSql( 
                trigger,
                `cannot replace freeze trigger ${ triggerIdentifySql }`
            );
            ddlSql = checkFreezeSql;


            ddlSql += ";";
            ddlSql += CreateTrigger.trigger2dropSql( trigger );
            
            ddlSql += ";";
            ddlSql += CreateTrigger.trigger2sql( trigger );

            ddlSql += ";";
            ddlSql += `
                comment on trigger ${ triggerIdentifySql } is 'ddl-manager-sync'
            `;

            try {
                await db.query(ddlSql);
            } catch(err) {
                out.errors.push(err);
            }
        }

        return out;
    }

    static async build({db, folder}) {
        let needCloseConnect = false;

        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);

            needCloseConnect = true;
        }
        
        let filesStateInstance = FilesState.create({
            folder,
            onError(err) {
                console.error(err.subPath + ": " + err.message);
            }
        });
        let filesState = {
            functions: filesStateInstance.getFunctions(),
            triggers: filesStateInstance.getTriggers()
        };
        
        let dbState = new DbState(db);
        await dbState.load();

        let diff = dbState.getDiff(filesState);


        await DdlManager.migrate(db, diff);

        if ( needCloseConnect ) {
            db.end();
        }

        logDiff(diff);
        
        console.log("ddl-manager build success");
        
        return filesStateInstance;
    }

    static async watch({db, folder}) {
        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);
        }

        let filesState = await DdlManager.build({db, folder});

        await filesState.watch();
        
        filesState.on("change", async(diff) => {
            try {
                await DdlManager.migrate(db, diff);

                logDiff(diff);
            } catch(err) {
                console.log(err);

                // если в файле была freeze функция
                // а потом в файле поменяли имя функции на другое (не freeze)
                // о новая функция должна быть создана, а freeze не должна быть сброшена
                // 
                let isFreezeDropError = (
                    /cannot drop freeze (trigger|function)/i.test(err.message)
                );
                let hasCreateFuncOrTrigger = (
                    diff.create.functions.length ||
                    diff.create.triggers.length
                );

                if ( isFreezeDropError && hasCreateFuncOrTrigger ) {
                    // т.к. DdlManager.migrate атомарная операция
                    // то запустим одтельно создание новой функции
                    // т.к. сборсить freeze нельзя, а новая функция может быть валидной

                    let createDiff = {
                        drop: {
                            functions: [],
                            triggers: []
                        },
                        create: diff.create
                    };

                    try {
                        await DdlManager.migrate(db, createDiff);
        
                        logDiff(diff);
                    } catch(err) {
                        console.log(err);
                    }
                }
            }
        });

        filesState.on("error", err => {
            console.log(err.message);
        });

        watchers.push(filesState);
    }

    static stopWatch() {
        watchers.forEach(watcher => {
            watcher.stopWatch();
        });
        watchers.splice(0, watchers.length);
    }
}

function isDbClient(dbOrConfig) {
    return (
        dbOrConfig && 
        _.isFunction(dbOrConfig.query)
    );
}

async function getDbClient(dbConfig) {
    let config = {
        database: false,
        user: false,
        password: false,
        host: "localhost",
        port: 5432
    };

    if ( "database" in dbConfig ) {
        config.database = dbConfig.database;
    }
    if ( "user" in dbConfig ) {
        config.user = dbConfig.user;
    }
    if ( "password" in dbConfig ) {
        config.password = dbConfig.password;
    }
    if ( "host" in dbConfig ) {
        config.host = dbConfig.host;
    }
    if ( "port" in dbConfig ) {
        config.port = dbConfig.port;
    }

    let dbClient = new pg.Client(config);
    await dbClient.connect();

    return dbClient;
}

function logDiff(diff) {
    diff.drop.triggers.forEach(trigger => {
        let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
        console.log("dropped trigger " + triggerIdentifySql);
    });

    diff.drop.functions.forEach(func => {
        let funcIdentifySql = CreateFunction.function2identifySql( func );
        console.log("dropped function " + funcIdentifySql);
    });
    
    diff.create.functions.forEach(func => {
        let funcIdentifySql = CreateFunction.function2identifySql( func );
        console.log("created function " + funcIdentifySql);
    });

    diff.create.triggers.forEach(trigger => {
        let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
        console.log("created trigger " + triggerIdentifySql);
    });
}

module.exports = DdlManager;