"use strict";

const DbState = require("./DbState");
const FilesState = require("./FilesState");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const CreateFunction = require("./parser/syntax/CreateFunction");
const CommentOn = require("./parser/syntax/CommentOn");
const fs = require("fs");
const {
    getDbClient, 
    logDiff, 
    isDbClient,
    findFunctionComment,
    findTriggerComment
} = require("./utils");

const watchers = [];

class DdlManager {
    static async migrate({db, diff, throwError}) {
        if ( diff == null ) {
            throw new Error("invalid diff");
        }
        let out = {
            errors: []
        };

        // drop old objects
        let dropComments = diff.drop.comments || [];
        for (let i = 0, n = dropComments.length; i < n; i++) {
            let ddlSql = "";
            let comment = dropComments[i];

            ddlSql += CommentOn.comment2dropSql(comment);

            try {
                await db.query(ddlSql);
            } catch(err) {
                // redefine callstack
                let newErr = new Error(err.message);
                newErr.originalError = err;
                
                out.errors.push(newErr);
            }
        }


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
                // redefine callstack
                let newErr = new Error(err.message);
                newErr.originalError = err;

                out.errors.push(newErr);
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
                // redefine callstack
                let newErr = new Error(err.message);
                newErr.originalError = err;
                
                out.errors.push(newErr);
            }
        }



        // create new objects
        let createComments = diff.create.comments || [];

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
            // comment on function ddl-manager-sync
            let comment = findFunctionComment(
                createComments,
                func
            );

            ddlSql += DbState.getUnfreezeFunctionSql(func, comment);

            try {
                await db.query(ddlSql);
            } catch(err) {
                // redefine callstack
                let newErr = new Error(err.message);
                newErr.originalError = err;
                
                out.errors.push(newErr);
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
            // comment on trigger ddl-manager-sync
            let comment = findTriggerComment(
                createComments,
                trigger
            );
            ddlSql += DbState.getUnfreezeTriggerSql(trigger, comment);

            try {
                await db.query(ddlSql);
            } catch(err) {
                // redefine callstack
                let newErr = new Error(err.message);
                newErr.originalError = err;
                
                out.errors.push(newErr);
            }
        }

        if ( throwError !== false ) {
            if ( out.errors.length ) {
                let err = out.errors[0];
                throw new Error(err);
            }
        }

        return out;
    }

    static async build({db, folder, throwError = true}) {
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
            triggers: filesStateInstance.getTriggers(),
            comments: filesStateInstance.getComments()
        };
        
        let dbState = new DbState(db);
        await dbState.load();

        let diff = dbState.getDiff(filesState);


        let migrateResult = await DdlManager.migrate({
            db, diff,
            throwError: false
        });

        if ( needCloseConnect ) {
            db.end();
        }

        logDiff(diff);
        
        if ( !migrateResult.errors.length ) {
            console.log("ddl-manager build success");
        }
        else if ( throwError ) {
            throw migrateResult.errors[0];
        }
        
        return filesStateInstance;
    }

    static async watch({db, folder}) {
        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);
        }

        let filesState = await DdlManager.build({
            db, 
            folder,
            throwError: false
        });

        await filesState.watch();
        
        filesState.on("change", async(diff) => {
            try {
                await DdlManager.migrate({
                    db, diff,
                    throwError: false
                });

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
                    // запустим одтельно создание новой функции
                    // т.к. сборсить freeze нельзя, а новая функция может быть валидной

                    let createDiff = {
                        drop: {
                            functions: [],
                            triggers: []
                        },
                        create: diff.create
                    };

                    try {
                        await DdlManager.migrate({
                            db, 
                            diff: createDiff,
                            throwError: false
                        });
        
                        logDiff(diff);
                    } catch(err) {
                        console.log(err);
                    }
                }
            }
        });

        watchers.push(filesState);
    }

    static async dump({db, folder, unfreeze = false}) {
        let needCloseConnect = false;

        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);

            needCloseConnect = true;
        }

        if ( !fs.existsSync(folder) ) {
            throw new Error(`folder "${ folder }" not found`);
        }

        let dbState = new DbState(db);
        await dbState.load();

        let dbComments = dbState.comments || [];

        let existsFolders = {};

        // functions from database
        let functions = dbState.functions.slice();

        for (let i = 0, n = functions.length; i < n; i++) {
            let func = functions[i];
            let sameFuncs = [func];
            let comment;

            // find functions with same name
            // expected sorted array by schema/name
            for (let j = i + 1; j < n; j++) {
                let nextFunc = functions[ j ];
                let isSame = (
                    nextFunc.schema == func.schema &&
                    nextFunc.name == func.name
                );

                if ( isSame ) {
                    sameFuncs.push( nextFunc );

                    // remove from stack
                    functions.splice(j, 1);

                    // after splice, length is less
                    j--;
                    n--;
                }
                else {
                    break;
                }
            }


            // generate sql
            let sql = "";
            let firstTrigger = false;

            sameFuncs.forEach((sameFunc, i) => {
                if ( i > 0 ) {
                    sql += ";\n";
                    sql += "\n";
                }

                sql += CreateFunction.function2sql(sameFunc);

                // custom comment from db, we write as 
                // comment on ...
                comment = findFunctionComment(
                    dbComments,
                    func
                );
    
                if ( comment ) {
                    sql += ";\n";
                    sql += "\n";
    
                    sql += CommentOn.comment2sql(comment);
                }
            });
            
            // file can contain triggers
            // find triggers, who call this func
            
            if ( func.returns.type == "trigger" ) {
                let triggers = dbState.triggers.filter(trigger =>
                    trigger.procedure.schema == func.schema &&
                    trigger.procedure.name == func.name
                );
    
                if ( triggers.length ) {
                    if ( !firstTrigger ) {
                        firstTrigger = triggers[0];
                    }
    
                    triggers.forEach(trigger => {
                        sql += ";\n";
                        sql += "\n";
        
                        sql += CreateTrigger.trigger2sql( trigger );
    
                        comment = findTriggerComment(
                            dbComments,
                            trigger
                        );
    
                        if ( comment ) {
                            sql += ";\n";
                            sql += "\n";
    
                            sql += CommentOn.comment2sql(comment);
                        }
                    });
                }
            }
            
            

            // create dirs and file
            let fileName = func.name + ".sql";

            // create folder public or some schema
            let subFolder = func.schema;
            if ( firstTrigger ) {
                subFolder = firstTrigger.table.schema;
            }
            let path = folder + "/" + subFolder;

            if ( !existsFolders[ subFolder ] ) {
                if ( !fs.existsSync(path) ) {
                    fs.mkdirSync(path);
                }
                existsFolders[ subFolder ] = true;
            }

            if ( firstTrigger ) {
                subFolder = firstTrigger.table.schema + "/" + firstTrigger.table.name;
                path = folder + "/" + subFolder;

                if ( !existsFolders[ subFolder ] ) {
                    if ( !fs.existsSync(path) ) {
                        fs.mkdirSync(path);
                    }
                    existsFolders[ subFolder ] = true;
                }
            }


            // save sql
            fs.writeFileSync(path + "/" + fileName, sql);
        }
        
        if ( unfreeze ) {
            await dbState.unfreezeAll();
        }

        if ( needCloseConnect ) {
            db.end();
        }
    }

    static stopWatch() {
        watchers.forEach(watcher => {
            watcher.stopWatch();
        });
        watchers.splice(0, watchers.length);
    }
}

module.exports = DdlManager;