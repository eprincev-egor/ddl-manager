import { Database } from "./database/schema/Database";
import { DatabaseTrigger } from "./database/schema/DatabaseTrigger";
import { DatabaseFunction } from "./database/schema/DatabaseFunction";
import { FilesState } from "./fs/FilesState";
import { Migration } from "./Migrator/Migration";
import { flatMap } from "lodash";

export class Comparator {

    static compare(database: Database, fs: FilesState) {
        const comparator = new Comparator(database, fs);
        return comparator.compare();
    }

    private migration: Migration;
    private database: Database;
    private fs: FilesState;

    private constructor(database: Database, fs: FilesState) {
        this.database = database;
        this.fs = fs;
        this.migration = Migration.empty();
    }

    compare() {
        this.dropOldObjects();
        this.createNewObjects();

        return this.migration;
    }

    private dropOldObjects() {
        this.dropOldTriggers();
        this.dropOldFunctions();
    }

    private dropOldTriggers() {
        for (const table of this.database.tables) {
            for (const dbTrigger of table.triggers) {
                
                if ( dbTrigger.frozen ) {
                    continue;
                }

                const existsSameTriggerFromFile = flatMap(this.fs.files, file => file.content.triggers).some(fileTrigger =>
                    fileTrigger.equal(dbTrigger)
                );

                if ( !existsSameTriggerFromFile ) {
                    this.migration.drop({
                        triggers: [dbTrigger]
                    });
                }
            }
        }
    }

    private dropOldFunctions() {
        for (const dbFunc of this.database.functions) {
            
            // ddl-manager cannot drop frozen function
            if ( dbFunc.frozen ) {
                continue;
            }

            const existsSameFuncFromFile = flatMap(this.fs.files, file => file.content.functions).some(fileFunc =>
                fileFunc.equal(dbFunc)
            );

            if ( existsSameFuncFromFile ) {
                continue;
            }

            // for drop function, need drop trigger, who call it function
            if ( dbFunc.returns.type === "trigger" ) {
                const depsTriggers = this.database.getTriggersByProcedure({
                    schema: dbFunc.schema,
                    name: dbFunc.name,
                    args: dbFunc.args.map(arg => arg.type)
                }).filter(dbTrigger => {
                    const isDepsTrigger = (
                        dbTrigger.procedure.schema === dbFunc.schema &&
                        dbTrigger.procedure.name === dbFunc.name
                    );

                    if ( !isDepsTrigger ) {
                        return false;
                    }
                    
                    const existsSameTriggerFromFile = flatMap(this.fs.files, file => file.content.triggers).some(fileTrigger =>
                        fileTrigger.equal(dbTrigger)
                    );

                    // if trigger has change, then he will dropped
                    // in next cycle
                    if ( !existsSameTriggerFromFile ) {
                        return false;
                    }

                    // we have trigger and he without changes
                    return true;
                });

                // drop
                this.migration.drop({triggers: depsTriggers});
                // and create again
                this.migration.create({triggers: depsTriggers});
            }
            
            
            this.migration.drop({
                functions: [dbFunc]
            });
        }
    }

    private createNewObjects() {
        for (const file of this.fs.files) {
            this.createNewFunctions( file.content.functions );
            this.createNewTriggers( file.content.triggers );
        }
    }

    private createNewFunctions(functions: DatabaseFunction[]) {
        for (const func of functions) {
            const existsSameFuncFromDb = this.database.functions.find(dbFunc =>
                dbFunc.equal(func)
            );

            if ( existsSameFuncFromDb ) {
                continue;
            }

            this.migration.create({
                functions: [func]
            });
        }
    }

    private createNewTriggers(triggers: DatabaseTrigger[]) {
        for (const trigger of triggers) {

            const dbTable = this.database.getTable(trigger.table);

            const existsSameTriggerFromDb = dbTable && dbTable.triggers.some(dbTrigger =>
                dbTrigger.equal(trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            this.migration.create({
                triggers: [trigger]
            });
        }
    }
}
