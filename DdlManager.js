"use strict";

const fs = require("fs");
const glob = require("glob");
const DDLCoach = require("./parser/DDLCoach");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const CreateFunction = require("./parser/syntax/CreateFunction");
const _ = require("lodash");
const pg = require("pg");

class DdlManager {
    static parseFolder(folderPath) {
        if ( !fs.existsSync(folderPath) ) {
            throw new Error(`folder "${ folderPath }" not found`);
        }

        // array of object:
        // {
        //   name: "some-file-name.sql",
        //   path: "/path/to/some-file-name.sql",
        //   content: {
        //        function: ...
        //   }
        // }
        let out = [];

        let files = glob.sync(folderPath + "/**/*.sql");
        files.forEach(filePath => {
            let fileName = filePath.split(/[/\\]/).pop();
            let content = DdlManager.parseFile(filePath);

            out.push({
                name: fileName,
                content
            });
        });

        return out;
    }

    static parseFile(filePath) {
        let fileContentBuffer;
        let fileContent;

        try {
            fileContentBuffer = fs.readFileSync(filePath);
        } catch(err) {
            throw new Error(`file "${ filePath }" not found`);
        }
        
        
        fileContent = fileContentBuffer.toString();
        fileContent = fileContent.trim();

        if ( fileContent === "" ) {
            return null;
        }

        let coach = new DDLCoach(fileContent);
        let func = coach.parseCreateFunction();

        let out = {
            function: func.toJSON()
        };
        out.function.freeze = false;

        coach.skipSpace();
        if ( coach.is(";") ) {
            coach.expect(";");
            coach.skipSpace();

            if ( coach.isCreateTrigger() ) {
                let trigger = coach.parseCreateTrigger();

                // validate function name and trigger procedure
                if ( 
                    out.function.schema != trigger.procedure.schema ||
                    out.function.name != trigger.procedure.name
                ) {
                    throw new Error(`wrong procedure name ${
                        trigger.procedure.schema
                    }.${
                        trigger.procedure.name
                    }`);
                }

                // validate function returns type
                if ( out.function.returns !== "trigger" ) {
                    throw new Error(`wrong returns type ${ out.function.returns }`);
                }
            
                out.trigger = trigger.toJSON();
                out.trigger.freeze = false;
            }
        }

        return out;
    }

    static async migrateFunction(db, func) {
        if ( func == null ) {
            throw new Error("invalid function");
        }

        let ddlSql = CreateFunction.function2sql( func );
        let funcIdentifySql = CreateFunction.function2identifySql( func );
        ddlSql += ";";
        ddlSql += `comment on function ${ funcIdentifySql } is 'ddl-manager-sync'`;
        ddlSql += ";";
        
        try { 
            await db.query(ddlSql);
            
            console.log("created function " + funcIdentifySql);

        } catch(err) {
            // redefine callstack
            let newErr = new Error(err.message);
            newErr.originalError = err;
            throw newErr;
        }
    }

    static async migrateTrigger(db, trigger) {
        if ( trigger == null ) {
            throw new Error("invalid trigger");
        }

        let ddlSql = CreateTrigger.trigger2dropSql(trigger);
        ddlSql += ";";
        ddlSql += CreateTrigger.trigger2sql(trigger);

        let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
        ddlSql += ";";
        ddlSql += `comment on trigger ${ triggerIdentifySql } is 'ddl-manager-sync';`;
        
        try { 
            await db.query(ddlSql);

            console.log("created trigger " + triggerIdentifySql);
        } catch(err) {
            // redefine callstack
            let newErr = new Error(err.message);
            newErr.originalError = err;
            throw newErr;
        }
    }

    static async loadState(db) {
        let result;
        let state = {
            functions: [],
            triggers: []
        };

        try { 
            result = await db.query(`
                select
                    pg_get_functiondef( pg_proc.oid ) as ddl,
                    pg_catalog.obj_description( pg_proc.oid ) as comment
                from information_schema.routines as routines

                left join pg_catalog.pg_proc as pg_proc on
                    routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text

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
            `);
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
                /ddl-manager-sync/i.test(row.comment)
            );
            json.freeze = !canSyncFunction;

            state.functions.push(json);
        });

        try { 
            result = await db.query(`
                select
                    pg_get_triggerdef( pg_trigger.oid ) as ddl,
                    pg_catalog.obj_description( pg_trigger.oid ) as comment
                from pg_trigger
            `);
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

            state.triggers.push(json);
        });

        return state;
    }

    static diffState({
        filesState,
        dbState
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
        if ( !_.isObject(dbState) ) {
            throw new Error("undefined folder");
        }
        if ( !_.isArray(dbState.functions) ) {
            throw new Error("undefined dbState.functions");
        }
        if ( !_.isArray(dbState.triggers) ) {
            throw new Error("undefined dbState.triggers");
        }


        let drop = {
            functions: [],
            triggers: []
        };
        let create = {
            functions: [],
            triggers: []
        };
        let freeze = {
            functions: [],
            triggers: []
        };

        
        for (let i = 0, n = dbState.functions.length; i < n; i++) {
            let func = dbState.functions[ i ];
            
            if ( func.freeze ) {
                freeze.functions.push(
                    func
                );
                continue;
            }

            let existsSameFuncFromFile = filesState.functions.some(fileFunc =>
                equalFunction(fileFunc, func)
            );

            if ( existsSameFuncFromFile ) {
                continue;
            }

            drop.functions.push(func);
        }
        for (let i = 0, n = dbState.triggers.length; i < n; i++) {
            let trigger = dbState.triggers[ i ];

            if ( trigger.freeze ) {
                freeze.triggers.push(
                    trigger
                );
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



        for (let i = 0, n = filesState.functions.length; i < n; i++) {
            let func = filesState.functions[ i ];
            let identifyFunc = CreateFunction.function2identifySql(func);

            let isFreezeFunction = freeze.functions.some(freezeFunc =>
                identifyFunc == CreateFunction.function2identifySql(freezeFunc)
            );

            if ( isFreezeFunction ) {
                throw new Error(`cannot replace freeze function ${identifyFunc}`);
            }

            let existsSameFuncFromDb = dbState.functions.find(dbFunc =>
                equalFunction(dbFunc, func)
            );

            if ( existsSameFuncFromDb ) {
                continue;
            }

            create.functions.push(func);
        }
        for (let i = 0, n = filesState.triggers.length; i < n; i++) {
            let trigger = filesState.triggers[ i ];
            let identifyTrigger = CreateTrigger.trigger2identifySql(trigger);

            let isFreezeTrigger = freeze.triggers.some(freezeTrigger =>
                identifyTrigger == CreateTrigger.trigger2identifySql(freezeTrigger)
            );

            if ( isFreezeTrigger ) {
                throw new Error(`cannot replace freeze trigger ${identifyTrigger}`);
            }

            let existsSameTriggerFromDb = dbState.triggers.some(dbTrigger =>
                equalTrigger(dbTrigger, trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            create.triggers.push( trigger );
        }



        return {
            drop,
            create,
            freeze
        };
    }

    static files2state(files) {
        let state = {
            functions: [],
            triggers: []
        };

        files.forEach(file => {
            let content = file.content;

            state.functions.push(
                content.function
            );
            
            let trigger = content.trigger;
            if ( trigger ) {
                trigger = _.cloneDeep(trigger);

                trigger.procedure = {
                    schema: content.function.schema,
                    name: content.function.name
                };

                state.triggers.push(trigger);
            }
        });

        return state;
    }

    static async build({db, folder}) {
        let needCloseConnect = false;

        // if db is config
        if ( db && !_.isFunction(db.query) ) {
            let dbConfig = {
                database: false,
                user: false,
                password: false,
                host: "localhost",
                port: 5432
            };

            if ( "database" in db ) {
                dbConfig.database = db.database;
            }
            if ( "user" in db ) {
                dbConfig.user = db.user;
            }
            if ( "password" in db ) {
                dbConfig.password = db.password;
            }
            if ( "host" in db ) {
                dbConfig.host = db.host;
            }
            if ( "port" in db ) {
                dbConfig.port = db.port;
            }

            db = new pg.Client(dbConfig);
            await db.connect();

            needCloseConnect = true;
        }
        
        
        let files = DdlManager.parseFolder(folder);
        let filesState = DdlManager.files2state(files);
        let dbState = await DdlManager.loadState(db);

        let diff = DdlManager.diffState({
            filesState,
            dbState
        });


        // objects, who created without ddl-manager
        let hasFreezeObjects = (
            diff.freeze.functions.length ||
            diff.freeze.triggers.length
        );
        if ( hasFreezeObjects ) {
            let freezeObjects = [];

            diff.freeze.functions.forEach(freezeFunc => {
                let identifySql = CreateFunction.function2identifySql( freezeFunc );
                freezeObjects.push(
                    identifySql
                );
            });

            diff.freeze.triggers.forEach(freezeTrigger => {
                let identifySql = CreateTrigger.trigger2identifySql( freezeTrigger );
                freezeObjects.push(
                    identifySql
                );
            });


            console.error(`
                found objects without file mirror:
                    ${ freezeObjects.join("\n") }
            `);
        }


        // drop old objects
        let dropSql = [];
        diff.drop.triggers.map(trigger => {
            dropSql.push(
                CreateTrigger.trigger2dropSql(trigger)
            );
        });
        diff.drop.functions.map(func => {
            dropSql.push(
                CreateFunction.function2dropSql(func)
            );
        });
        dropSql = dropSql.join(";\n\n");

        await db.query(dropSql);

        diff.drop.triggers.map(trigger => {
            let triggerIdentifySql = CreateTrigger.trigger2identifySql(trigger);
            console.log("dropped trigger " + triggerIdentifySql);
        });
        diff.drop.functions.map(func => {
            let funcIdentifySql = CreateFunction.function2identifySql(func);
            console.log("dropped function " + funcIdentifySql);
        });

        
        // create new objects
        for (let i = 0, n = diff.create.functions.length; i < n; i++) {
            let func = diff.create.functions[ i ];

            let trigger = filesState.triggers.find(trigger =>
                trigger.procedure.schema == func.schema &&
                trigger.procedure.name == func.name
            );

            await DdlManager.migrateFunction(db, func);

            if ( trigger ) {
                await DdlManager.migrateTrigger(db, trigger);
            }
        }

        if ( needCloseConnect ) {
            db.end();
        }

        console.log("ddl-manager build success");
    }
}

function equalFunction(func1, func2) {
    func1 = _.cloneDeep(func1);
    func2 = _.cloneDeep(func2);

    if ( !func1.freeze ) {
        delete func1.freeze;
    }

    if ( !func2.freeze ) {
        delete func2.freeze;
    }

    return JSON.stringify(func1) == JSON.stringify(func2);
}

function equalTrigger(trigger1, trigger2) {
    trigger1 = _.cloneDeep(trigger1);
    trigger2 = _.cloneDeep(trigger2);

    if ( !trigger1.freeze ) {
        delete trigger1.freeze;
    }
    
    if ( !trigger2.freeze ) {
        delete trigger2.freeze;
    }

    return JSON.stringify(trigger1) == JSON.stringify(trigger2);
}

module.exports = DdlManager;