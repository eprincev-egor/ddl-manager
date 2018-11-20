"use strict";

const fs = require("fs");
const glob = require("glob");
const DDLCoach = require("./parser/DDLCoach");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const CreateFunction = require("./parser/syntax/CreateFunction");
const _ = require("lodash");

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
            
                out.trigger = {
                    table: {
                        schema: trigger.table.schema,
                        name: trigger.table.name
                    }
                };
    
                if ( trigger.before ) {
                    out.trigger.before = true;
                }
                else if ( trigger.after ) {
                    out.trigger.after = true;
                }
    
                if ( trigger.insert ) {
                    out.trigger.insert = true;
                }
                if ( trigger.update ) {
                    if ( trigger.update === true ) {
                        out.trigger.update = true;
                    } else {
                        out.trigger.update = trigger.update.map(name => name);
                    }
                }
                if ( trigger.delete ) {
                    out.trigger.delete = true;
                }
            }
        }

        return out;
    }

    static async migrateFile(db, file) {
        if ( file == null ) {
            throw new Error("invalid function");
        }

        let ddlSql = CreateFunction.function2sql( file.function );
        let funcIdentifySql = CreateFunction.function2identifySql( file.function );
        ddlSql += ";";
        ddlSql += `comment on function ${ funcIdentifySql } is 'ddl-manager-sync'`;
        ddlSql += ";";
        
        if ( file.trigger ) {
            let trigger = _.cloneDeep(file.trigger);
            trigger.procedure = {
                schema: file.function.schema,
                name: file.function.name
            };
            
            ddlSql += CreateTrigger.trigger2dropSql(trigger);
            ddlSql += ";";
            ddlSql += CreateTrigger.trigger2sql(trigger);

            let triggerIdentifySql = CreateTrigger.trigger2identifySql( file.trigger );
            ddlSql += ";";
            ddlSql += `comment on trigger ${ triggerIdentifySql } is 'ddl-manager-sync';`;
        }
        
        try { 
            await db.query(ddlSql);
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
                    routines.routine_schema <> 'information_schema'
                
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
}

function equalFunction(func1, func2) {
    return JSON.stringify(func1) == JSON.stringify(func2);
}

function equalTrigger(trigger1, trigger2) {
    return JSON.stringify(trigger1) == JSON.stringify(trigger2);
}

module.exports = DdlManager;