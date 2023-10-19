import fs from "fs";
import pg from "pg";
import path from "path";
import { FileReader } from "./fs/FileReader";
import { FileWatcher } from "./fs/FileWatcher";
import { FilesState } from "./fs/FilesState";
import { MainComparator } from "./Comparator/MainComparator";
import { MainMigrator } from "./Migrator/MainMigrator";
import { Migration } from "./Migrator/Migration";
import { IDatabaseDriver } from "./database/interface";
import { PostgresDriver } from "./database/PostgresDriver";
import { Database } from "./database/schema/Database";
import { getDbClient, IDBConfig } from "./database/getDbClient";
import { DatabaseTrigger } from "./database/schema/DatabaseTrigger";
import { FunctionsMigrator } from "./Migrator/FunctionsMigrator";
import { createCallsTable, clearCallsLogs, downloadLogs } from "./timeline/callsTable";
import { parseCalls } from "./timeline/Coach";
import { createTimelineFile } from "./timeline/createTimelineFile";
import { CacheComparator, IFindBrokenColumnsParams, TimeRange } from "./Comparator/CacheComparator";

const watchers: FileWatcher[] = [];
interface IParams {
    db: IDBConfig | pg.Pool;
    folder: string | string[];
    throwError?: boolean;
}

interface ITimelineParams extends IParams {
    runOnlyScenario?: string;
    scenariosPath: string;
    outputPath: string;
}

export interface IRefreshCacheParams extends IParams, UpdateHooks {
    concreteTables?: string | string[];
    concreteColumns?: string | string[];
    timeoutPerUpdate?: number;
    
    timeoutBetweenUpdates?: number;
    /** deprecated, need use timeoutBetweenUpdates */
    timeoutOnUpdate?: number;

    updatePackageSize?: number;
    logToFile?: string;
}

export interface UpdateHooks {
    onStartUpdate?: (start: IUpdateStart) => void;
    onUpdate?: (result: IUpdateResult) => void;
    onUpdateError?: (result: IUpdateError) => void;
}

export interface IUpdateStart {
    columns: string[];
    rows: "package" | "recursion" | {minId: number; maxId: number};
}

export interface IUpdateResult extends IUpdateStart {
    time: TimeRange;
}

export interface IUpdateError extends IUpdateResult {
    error: Error;
}

export interface IScanBrokenParams extends IParams, IFindBrokenColumnsParams {}

export class DDLManager {

    static async build(params: IParams) {
        const ddlManager = new DDLManager(params);
        return await ddlManager.build();
    }

    static async compare(params: IParams) {
        const ddlManager = new DDLManager(params);
        const {migration} = await ddlManager.compareDbAndFs();
        return migration;
    }

    static async compareCache(params: IParams) {
        const ddlManager = new DDLManager(params);
        return await ddlManager.compareCache();
    }

    static async timeline(params: ITimelineParams) {
        const ddlManager = new DDLManager(params);
        return await ddlManager.timeline(params);
    }

    static async refreshCache(params: IRefreshCacheParams) {
        const ddlManager = new DDLManager(params);
        return await ddlManager.refreshCache(params);
    }

    static async scanBrokenColumns(params: IScanBrokenParams) {
        const ddlManager = new DDLManager(params);
        return await ddlManager.scanBrokenColumns(params);
    }

    static async watch(params: {
        db: IDBConfig,
        folder: string | string[]
    }) {
        const ddlManager = new DDLManager({
            db: params.db,
            folder: params.folder
        });

        return await ddlManager.watch();
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
    private dbConfig: IDBConfig | pg.Pool;
    private activeMigrators: MainMigrator[] = [];

    private constructor(params: IParams) {

        if ( typeof params.folder === "string" ) {
            this.folders = [params.folder];
        }
        else {
            this.folders = params.folder;
        }

        this.folders = this.folders.map(folderPath =>
            path.normalize(folderPath)
        );

        this.needCloseConnect = !("query" in params.db);
        this.dbConfig = params.db;
        this.needThrowError = !!params.throwError;
    }

    private async build() {
        const {migration, database, postgres} = await this.compareDbAndFs();

        const migrateErrors = await MainMigrator.migrate(
            postgres,
            database,
            migration
        );

        this.onMigrate(
            postgres,
            migration,
            migrateErrors
        );
    }

    // TODO: need tests and refactor
    private async timeline(params: ITimelineParams) {

        console.log("reading scenarios");
        let scenarios = readScenarios(params.scenariosPath);
        if ( params.runOnlyScenario ) {
            console.log("run only " + params.runOnlyScenario);
            scenarios = scenarios.filter(scenario =>
                scenario.name === params.runOnlyScenario
            );
        }
        if ( !scenarios.length ) {
            throw new Error("scenarios not found");
        }


        const db = await getDbClient(this.dbConfig);
        const filesState = await this.readFS();
        const postgres = await this.postgres();
        let database = await postgres.load();

        const migration = await MainComparator.logAllFuncsMigration(
            postgres,
            database,
            filesState
        );

        await createCallsTable(db);

        console.log("logging all funcs");
        const migrationErrors: Error[] = [];
        const functionsMigrator = new FunctionsMigrator(
            postgres,
            migration,
            database,
            migrationErrors
        );
        await functionsMigrator.createLogFuncs();


        for (const scenario of scenarios) {

            console.log("try run scenario " + scenario.name);
            try {
                await testTimelineScenario(
                    params.outputPath,
                    db,
                    scenario
                );
            }
            catch(err: any) {
                console.error(err);
                throw new Error("failed scenario " + scenario.name + " with error: " + err.message);
            }
        }

        console.log("unlogging all funcs");
        database = await postgres.load();
        const unlogMigration = await MainComparator.compare(
            postgres,
            database,
            filesState
        );
        await MainMigrator.migrate(
            postgres,
            database,
            unlogMigration
        );

        console.log("success");
    }

    private async refreshCache(params: IRefreshCacheParams) {
        if ( params.logToFile ) {
            fs.writeFileSync(params.logToFile, `[${new Date().toISOString()}] started\n`);
        }

        const filesState = await this.readFS();
        const postgres = await this.postgres();
        const database = await postgres.load();

        const migration = await MainComparator.refreshCache(
            postgres,
            database,
            filesState,
            params.concreteTables?.toString() ?? 
                params.concreteColumns?.toString()
        );
        
        const timeoutBetweenUpdates = params.timeoutBetweenUpdates || params.timeoutOnUpdate;
        if ( timeoutBetweenUpdates ) {
            migration.setTimeoutBetweenUpdates(timeoutBetweenUpdates);
        }
        if ( params.timeoutPerUpdate ) {
            migration.setTimeoutPerUpdate(params.timeoutPerUpdate);
        }
        if ( params.updatePackageSize ) {
            migration.setUpdatePackageSize(params.updatePackageSize);
        }
        if ( params.logToFile ) {
            migration.logToFile(params.logToFile);
        }
        migration.setUpdateHooks(params);

        const migrateErrors = await MainMigrator.migrate(
            postgres,
            database,
            migration
        );
        
        this.onMigrate(
            postgres,
            migration,
            migrateErrors
        );

        if ( params.logToFile ) {
            fs.appendFileSync(params.logToFile, `\n[${new Date().toISOString()}] finished`);
        }
    }

    private async compareCache() {
        const filesState = await this.readFS();
        const postgres = await this.postgres();
        const database = await postgres.load();

        const cacheComparator = new CacheComparator(
            postgres,
            database,
            filesState
        );
        const columns = cacheComparator.findChangedColumns();
        return columns;
    }

    private async scanBrokenColumns(params: IScanBrokenParams) {
        const filesState = await this.readFS();
        const postgres = await this.postgres();
        const database = await postgres.load();

        const cacheComparator = new CacheComparator(
            postgres,
            database,
            filesState
        );
        const columns = await cacheComparator.findBrokenColumns(params);
        return columns;
    }

    private async compareDbAndFs() {
        const filesState = await this.readFS();
        const postgres = await this.postgres();
        const database = await postgres.load();

        const migration = await MainComparator.compare(
            postgres,
            database,
            filesState
        );
        return {migration, postgres, database};
    }

    private readFS(): Promise<FilesState> {
        return new Promise((resolve, reject) => {
            const filesState = FileReader.read(
                this.folders,
                (err: Error) => {
                    if ( this.needThrowError ) {
                        reject(err);
                    }
                    else {
                        console.log(err);
                    }
                }
            );
            resolve(filesState);
        })
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
        const postgres = await this.postgres();
        const database = await postgres.load();
        const fs = await FileWatcher.watch(this.folders);

        const state = {
            database, fs
        };

        fs.on("change", () => {
            void this.onChangeFs(postgres, state)
        });
        fs.on("error", (err) => {
            console.error(err.message);
        });

        watchers.push(fs);

        return fs;
    }

    private async onChangeFs(
        postgres: IDatabaseDriver,
        state: {
            database: Database,
            fs: FileWatcher
        }
    ) {
        const needAbort = this.activeMigrators.length > 0;

        if ( needAbort ) {
            state.database = await postgres.load();
        }

        const migration = await MainComparator.compare(
            postgres,
            state.database,
            state.fs.state
        );
        const migrator = new MainMigrator(
            postgres,
            state.database,
            migration
        );

        for (const migrator of this.activeMigrators) {
            console.log("-- aborted migration --")
            migrator.abort();
        }
        this.activeMigrators = [migrator];

        const migrateErrors = await migrator.migrate();

        if ( !migrator.isAborted() ) {
            state.database.applyMigration(migration);

            migration.log();
            if ( migrateErrors.length ) {
                console.error(migrateErrors);
            }
        }

        this.activeMigrators = this.activeMigrators.filter(activeMigrator =>
            activeMigrator !== migrator
        );
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

                sql += sameFunc.toSQL();
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
        
                        sql += trigger.toSQL();
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

    private async postgres(): Promise<IDatabaseDriver> {
        const db = await getDbClient(this.dbConfig);
        const postgres = new PostgresDriver(db);
        return postgres;
    }
}

function readScenarios(scenariosPath: string) {
    const scenarios = [];

    const dirs = fs.readdirSync(scenariosPath);
    for (const scenarioDirName of dirs) {
        const scenario = readScenario(scenariosPath, scenarioDirName);
        scenarios.push(scenario);
    }
    
    return scenarios;
}

function readScenario(
    scenariosPath: string,
    scenarioDirName: string
) {
    const beforeSQLPath = path.join(scenariosPath, scenarioDirName, "before.sql");
    const sqlPath = path.join(scenariosPath, scenarioDirName, "test.sql");

    const beforeSQL = fs.readFileSync(beforeSQLPath).toString();
    const sql = fs.readFileSync(sqlPath).toString();

    return {
        name: scenarioDirName,
        beforeSQL, 
        sql
    };
}

async function testTimelineScenario(
    outputPath: string,
    db: pg.Pool,
    scenario = {
        name: "test",
        beforeSQL: "select 1",
        sql: "select 1"
    }
) {

    await db.query(scenario.beforeSQL);
    await clearCallsLogs(db);

    await db.query(scenario.sql);
    
    const logs = await downloadLogs(db);
    let rootCalls = parseCalls(logs);

    createTimelineFile({
        rootCalls,
        outputPath,
        name: scenario.name
    });
}
