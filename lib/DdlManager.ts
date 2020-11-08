import { DbState } from "./DbState";
import { FilesState } from "./FilesState";
import fs from "fs";
import path from "path";
import {
    getDbClient, 
    logDiff, 
    isDbClient,
    findCommentByFunction,
    findCommentByTrigger,
    trigger2sql,
    comment2sql,
    function2sql
} from "./utils";
import { Comparator } from "./Comparator";
import { Migrator } from "./Migrator";

const watchers: FilesState[] = [];

export class DdlManager {
    // TODO: any => type
    private static async migrate(params: {db: any, diff: any, throwError?: boolean}) {
        const {db, diff, throwError} = params;

        const migrator = new Migrator(db);
        const output = migrator.migrate(diff, throwError);
        return output;
    }

    static async build(params: {
        db: any;
        folder: string | string[];
        throwError?: boolean;
    }) {
        let db = params.db;

        let folder!: string[];
        if ( typeof params.folder === "string" ) {
            folder = [params.folder];
        }
        else {
            folder = params.folder;
        }
        folder = folder.map(folderPath =>
            path.normalize(folderPath)
        );

        let needCloseConnect = false;

        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);

            needCloseConnect = true;
        }
        
        const filesStateInstance = FilesState.create({
            folder,
            onError(err: Error) {
                // tslint:disable-next-line: no-console
                console.error((err as any).subPath + ": " + err.message);
            }
        });
        const filesState = {
            functions: filesStateInstance.getFunctions(),
            triggers: filesStateInstance.getTriggers(),
            comments: filesStateInstance.getComments()
        };
        
        const dbState = new DbState(db);
        await dbState.load();

        const comparator = new Comparator();
        const diff = comparator.compare(dbState, filesState);


        const migrateErrors = await DdlManager.migrate({
            db, diff,
            throwError: false
        });

        if ( needCloseConnect ) {
            db.end();
        }

        logDiff(diff);
        
        if ( !migrateErrors.length ) {
            // tslint:disable-next-line: no-console
            console.log("ddl-manager build success");
        }
        else if ( params.throwError ) {
            throw migrateErrors[0];
        }
        
        return filesStateInstance;
    }

    // TODO: any => type
    static async watch(params: {db: any, folder: string | string[]}) {
        const {folder} = params;
        let {db} = params;
    
        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);
        }

        const filesState = await DdlManager.build({
            db, 
            folder,
            throwError: false
        });

        await filesState.watch();
        
        filesState.on("change", async(diff) => {
            try {
                await DdlManager.migrate({
                    db, diff,
                    throwError: true
                });

                logDiff(diff);
            } catch(err) {

                // если в файле была freeze функция
                // а потом в файле поменяли имя функции на другое (не freeze)
                // о новая функция должна быть создана, а freeze не должна быть сброшена
                // 
                const isFreezeDropError = (
                    /cannot drop freeze (trigger|function)/i.test(err.message)
                );
                const hasCreateFuncOrTrigger = (
                    diff.create.functions.length ||
                    diff.create.triggers.length
                );

                if ( isFreezeDropError && hasCreateFuncOrTrigger ) {
                    // запустим одтельно создание новой функции
                    // т.к. сборсить freeze нельзя, а новая функция может быть валидной

                    const createDiff = {
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
                            throwError: true
                        });
        
                        logDiff(diff);
                    } catch(err) {
                        // tslint:disable-next-line: no-console
                        console.error(err);
                    }
                }
                else {
                    // tslint:disable-next-line: no-console
                    console.error(err);
                }
            }
        });

        watchers.push(filesState);
    }

    // TODO: any => type
    static async dump(params: {db: any, folder: string, unfreeze?: boolean}) {
        let {db} = params;
        const {folder, unfreeze} = params;

        let needCloseConnect = false;

        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);

            needCloseConnect = true;
        }

        if ( !fs.existsSync(folder) ) {
            throw new Error(`folder "${ folder }" not found`);
        }

        const dbState = new DbState(db);
        await dbState.load();

        const dbComments = dbState.comments || [];

        const existsFolders: {
            [dirPath: string]: boolean
        } = {};

        // functions from database
        const functions = dbState.functions.slice();

        for (let i = 0, n = functions.length; i < n; i++) {
            const func = functions[i];
            const sameFuncs = [func];
            let comment;

            // find functions with same name
            // expected sorted array by schema/name
            for (let j = i + 1; j < n; j++) {
                const nextFunc = functions[ j ];
                const isSame = (
                    nextFunc.schema === func.schema &&
                    nextFunc.name === func.name
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
            // TODO: any => type
            let firstTrigger: any = false;

            sameFuncs.forEach((sameFunc, j: number) => {
                if ( j > 0 ) {
                    sql += ";\n";
                    sql += "\n";
                }

                sql += function2sql(sameFunc);

                // custom comment from db, we write as 
                // comment on ...
                comment = findCommentByFunction(
                    dbComments,
                    sameFunc
                );
    
                if ( comment ) {
                    sql += ";\n";
                    sql += "\n";
    
                    sql += comment2sql(comment);
                }
            });
            
            // file can contain triggers
            // find triggers, who call this func
            
            const isTrigger = sameFuncs.some(sameFunc =>
                sameFunc.returns.type === "trigger"
            );
            if ( isTrigger ) {
                const triggers = dbState.triggers.filter(trigger =>
                    trigger.procedure.schema === func.schema &&
                    trigger.procedure.name === func.name
                );
    
                if ( triggers.length ) {
                    if ( !firstTrigger ) {
                        firstTrigger = triggers[0];
                    }
    
                    triggers.forEach(trigger => {
                        sql += ";\n";
                        sql += "\n";
        
                        sql += trigger2sql( trigger );
    
                        comment = findCommentByTrigger(
                            dbComments,
                            trigger
                        );
    
                        if ( comment ) {
                            sql += ";\n";
                            sql += "\n";
    
                            sql += comment2sql(comment);
                        }
                    });
                }
            }
            
            

            // create dirs and file
            const fileName = func.name + ".sql";

            // create folder public or some schema
            let subFolder = func.schema;
            if ( firstTrigger ) {
                subFolder = firstTrigger.table.schema;
            }
            let dirPath = folder + "/" + subFolder;

            if ( !existsFolders[ subFolder ] ) {
                if ( !fs.existsSync(dirPath) ) {
                    fs.mkdirSync(dirPath);
                }
                existsFolders[ subFolder ] = true;
            }

            if ( firstTrigger ) {
                subFolder = firstTrigger.table.schema + "/" + firstTrigger.table.name;
                dirPath = folder + "/" + subFolder;

                if ( !existsFolders[ subFolder ] ) {
                    if ( !fs.existsSync(dirPath) ) {
                        fs.mkdirSync(dirPath);
                    }
                    existsFolders[ subFolder ] = true;
                }
            }


            // save sql
            fs.writeFileSync(dirPath + "/" + fileName, sql + ";");
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
