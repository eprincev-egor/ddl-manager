import { AbstractComparator } from "./AbstractComparator";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { Migration } from "../Migrator/Migration";
import { IOutputTrigger } from "../cache/CacheTriggersBuilder";

export class FunctionsComparator extends AbstractComparator {

    constructor(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState,
        migration: Migration,
        private allCacheTriggers: IOutputTrigger[]
    ) {
        super(driver, database, fs, migration);
    }

    drop() {
        for (const dbFunc of this.database.functions) {
            // ddl-manager cannot drop frozen function
            if ( dbFunc.comment.frozen ) {
                continue;
            }

            const existsSameFuncFromFile = this.getFsFunctions(dbFunc.name).some(fileFunc =>
                fileFunc.equal(dbFunc)
            );
            if ( !existsSameFuncFromFile ) {
                this.migration.drop({
                    functions: [dbFunc]
                });
            }
        }
    }

    create() {
        for (const file of this.fs.files) {
            this.createNewFunctions( file.content.functions );
        }

        const cacheFunctions = this.allCacheTriggers.map(item => item.function);
        this.createNewFunctions(cacheFunctions);
    }

    createLogFuncs() {
        this.migration.create({
            functions: [
                ...this.fs.allFunctions(),
                ...this.allCacheTriggers.map(item => item.function)
            ]
        });
    }

    private createNewFunctions(functions: DatabaseFunction[]) {
        for (const fsFunc of functions) {
            const existsSameFuncFromDb = this.database.getFunctions(fsFunc.name).some(dbFunc =>
                dbFunc.equal(fsFunc)
            );

            if ( !existsSameFuncFromDb ) {
                this.migration.create({
                    functions: [fsFunc]
                });
            }
        }
    }

    private getFsFunctions(funcName: string) {
        const sameNameFsFunctions = this.fs.getFunctionsByName(funcName);
        const sameNameCacheFunctions = this.allCacheTriggers.filter(item => 
            item.function.name === funcName
        ).map(item => item.function);

        return [...sameNameCacheFunctions, ...sameNameFsFunctions];
    }
}