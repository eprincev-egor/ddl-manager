import { FileReader } from "./fs/FileReader";
import fs from "fs";
import pg from "pg";
import path from "path";
import { Comparator } from "./Comparator/Comparator";
import { MainMigrator } from "./Migrator/MainMigrator";
import { IDatabaseDriver } from "./database/interface";
import { PostgresDriver } from "./database/PostgresDriver";
import { getDbClient, IDBConfig } from "./database/getDbClient";
import { DatabaseTrigger } from "./database/schema/DatabaseTrigger";
import { Migration } from "./Migrator/Migration";

const watchers: FileReader[] = [];

export class DDLManager {

    static async build(params: {
        db: IDBConfig | pg.Client;
        folder: string | string[];
        throwError?: boolean;
    }) {
        const ddlManager = new DDLManager({
            db: params.db,
            folder: params.folder,
            throwError: params.throwError
        });
        return await ddlManager.build();
    }

    static async refreshCache(params: {
        db: IDBConfig | pg.Client;
        folder: string | string[];
        throwError?: boolean;
    }) {
        const ddlManager = new DDLManager({
            db: params.db,
            folder: params.folder,
            throwError: params.throwError
        });
        return await ddlManager.refreshCache();
    }

    static async watch(params: {
        db: IDBConfig,
        folder: string | string[]
    }) {
        const ddlManager = new DDLManager({
            db: params.db,
            folder: params.folder
        });

        await ddlManager.watch();
    }

    static async dump(params: {
        db: IDBConfig,
        folder: string,
        unfreeze?: boolean
    }) {
        const ddlManager = new DDLManager({
            db: params.db,
            folder: params.folder
        });
        return await ddlManager.dump(params.unfreeze);
    }

    static stopWatch() {
        watchers.forEach(watcher => {
            watcher.stopWatch();
        });
        watchers.splice(0, watchers.length);
    }

    private folders: string[];
    private needThrowError: boolean;
    private needCloseConnect: boolean;
    private dbConfig: IDBConfig | pg.Client;

    private constructor(params: {
        db: IDBConfig | pg.Client;
        folder: string | string[];
        throwError?: boolean;
    }) {

        if ( typeof params.folder === "string" ) {
            this.folders = [params.folder];
        }
        else {
            this.folders = params.folder;
        }

        this.folders = this.folders.map(folderPath =>
            path.normalize(folderPath)
        );

        this.needCloseConnect = !(params.db instanceof pg.Client);
        this.dbConfig = params.db;
        this.needThrowError = !!params.throwError;
    }

    private async build() {
        const {migration, postgres, fileReader} = await this.compareDbAndFs();

        const migrateErrors = await MainMigrator.migrate(postgres, migration);

        this.onMigrate(
            postgres,
            migration,
            migrateErrors
        );
        
        return fileReader;
    }

    private async refreshCache() {
        const {migration, postgres} = await this.compareDbAndFs();

        const migrateErrors = await MainMigrator.refreshCache(postgres, migration);
        
        this.onMigrate(
            postgres,
            migration,
            migrateErrors
        );
    }

    private async compareDbAndFs() {
        const fileReader = FileReader.read({
            folder: this.folders,
            onError(err: Error) {
                // tslint:disable-next-line: no-console
                console.error((err as any).subPath + ": " + err.message);
            }
        });
        
        const postgres = await this.postgres();
        const databaseState = await postgres.load();

        const migration = Comparator.compare(databaseState, fileReader.state);
        return {migration, postgres, fileReader};
    }

    private onMigrate(
        postgres: IDatabaseDriver,
        migration: Migration,
        migrateErrors: Error[]
    ) {
        if ( this.needCloseConnect ) {
            postgres.end();
        }

        migration.log();
        
        if ( !migrateErrors.length ) {
            // tslint:disable-next-line: no-console
            console.log("ddl-manager build success");
        }
        else if ( this.needThrowError ) {
            throw migrateErrors[0];
        }
    }

    private async watch() {
        const filesState = await this.build();

        await filesState.watch();
        
        filesState.on("change", (migration: Migration) => {
            this.onChangeFS(migration);
        });

        watchers.push(filesState);
    }

    private async onChangeFS(migration: Migration) {

        try {
            await this.migrateWithoutUpdateCacheColumns(migration);

            migration.log();
        } catch(err) {

            // если в файле была frozen функция
            // а потом в файле поменяли имя функции на другое (не frozen)
            // о новая функция должна быть создана, а frozen не должна быть сброшена
            // 
            const isFrozenDropError = (
                /cannot drop frozen (trigger|function)/i.test(err.message)
            );
            const hasCreateFuncOrTrigger = (
                migration.toCreate.functions.length ||
                migration.toCreate.triggers.length
            );

            if ( isFrozenDropError && hasCreateFuncOrTrigger ) {
                // запустим одтельно создание новой функции
                // т.к. сборсить frozen нельзя, а новая функция может быть валидной

                const createDiff = Migration.empty()
                    .create(migration.toCreate);

                try {
                    await this.migrateWithoutUpdateCacheColumns(createDiff);
    
                    migration.log();
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
    }

    private async dump(unfreeze: boolean = false) {
        const folder = this.folders[0] as string;
        if ( !fs.existsSync(folder) ) {
            throw new Error(`folder "${ folder }" not found`);
        }

        const postgres = await this.postgres();
        const dbState = await postgres.load();

        const existsFolders: {
            [dirPath: string]: boolean
        } = {};

        // functions from database
        const functions = dbState.functions.slice();

        for (let i = 0, n = functions.length; i < n; i++) {
            const func = functions[i];
            const sameFuncs = [func];

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
            let firstTrigger: DatabaseTrigger | false = false;

            sameFuncs.forEach((sameFunc, j: number) => {
                if ( j > 0 ) {
                    sql += ";\n";
                    sql += "\n";
                }

                sql += sameFunc.toSQLWithComment();
            });
            
            // file can contain triggers
            // find triggers, who call this func
            
            const isTrigger = sameFuncs.some(sameFunc =>
                sameFunc.returns.type === "trigger"
            );
            if ( isTrigger ) {
                const triggers = dbState.getTriggersByProcedure({
                    schema: func.schema,
                    name: func.name,
                    args: func.args.map(arg => arg.type)
                });

                if ( triggers.length ) {
                    if ( !firstTrigger ) {
                        firstTrigger = triggers[0];
                    }
    
                    triggers.forEach(trigger => {
                        sql += ";\n";
                        sql += "\n";
        
                        sql += trigger.toSQLWithComment();
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
            await postgres.unfreezeAll(dbState);
        }

        if ( this.needCloseConnect ) {
            postgres.end();
        }
    }

    private async migrateWithoutUpdateCacheColumns(migration: Migration) {
        const postgres = await this.postgres();
        const outputErrors = await MainMigrator.migrateWithoutUpdateCacheColumns(
            postgres, migration
        );

        if ( outputErrors.length ) {
            const err = outputErrors[0];
            throw new Error(err.message);
        }
    }

    private async postgres(): Promise<IDatabaseDriver> {
        const db = await getDbClient(this.dbConfig);
        const postgres = new PostgresDriver(db);
        return postgres;
    }
}
