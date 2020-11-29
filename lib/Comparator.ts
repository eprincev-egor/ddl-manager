import { flatMap } from "lodash";
import { Database } from "./database/schema/Database";
import { FilesState } from "./fs/FilesState";
import { Diff } from "./Diff";

export class Comparator {

    static compare(database: Database, fs: FilesState) {
        const comparator = new Comparator(database, fs);
        return comparator.compare();
    }

    private diff: Diff;
    private database: Database;
    private fs: FilesState;

    private constructor(database: Database, fs: FilesState) {
        this.diff = Diff.empty();
        this.database = database;
        this.fs = fs;
    }

    compare() {
        this.dropFunctions();
        this.dropTriggers();
        
        this.createFunctions();
        this.createTriggers();

        return this.diff;
    }

    private dropFunctions() {
        for (const func of this.database.functions) {
            
            // ddl-manager cannot drop frozen function
            if ( func.frozen ) {
                continue;
            }

            const existsSameFuncFromFile = flatMap(this.fs.files, file => file.content.functions).some(fileFunc =>
                fileFunc.equal(func)
            );

            if ( existsSameFuncFromFile ) {
                continue;
            }

            // for drop function, need drop trigger, who call it function
            if ( func.returns.type === "trigger" ) {
                const depsTriggers = this.database.getTriggersByProcedure({
                    schema: func.schema,
                    name: func.name,
                    args: func.args.map(arg => arg.type)
                }).filter(dbTrigger => {
                    const isDepsTrigger = (
                        dbTrigger.procedure.schema === func.schema &&
                        dbTrigger.procedure.name === func.name
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
                this.diff.drop({triggers: depsTriggers});
                // and create again
                this.diff.create({triggers: depsTriggers});
            }
            
            
            this.diff.dropFunction(func);
        }
    }

    private dropTriggers() {
        const triggersCreatedFromDDLManager = flatMap(this.database.tables, table => table.triggers).filter(trigger =>
            !trigger.frozen
        );
        const triggersToDrop = triggersCreatedFromDDLManager.filter(trigger => {

            const existsSameTriggerFromFile = flatMap(this.fs.files, file => file.content.triggers).some(fileTrigger =>
                fileTrigger.equal(trigger)
            );
            return !existsSameTriggerFromFile;
        });

        this.diff.drop({triggers: triggersToDrop});
    }

    private createFunctions() {
        for (const func of flatMap(this.fs.files, file => file.content.functions)) {

            const existsSameFuncFromDb = this.database.functions.find(dbFunc =>
                dbFunc.equal(func)
            );

            if ( existsSameFuncFromDb ) {
                continue;
            }

            this.diff.createFunction(func);
        }
    }

    private createTriggers() {
        for (const trigger of flatMap(this.fs.files, file => file.content.triggers)) {
            
            const existsSameTriggerFromDb = flatMap(this.database.tables, table => table.triggers).some(dbTrigger =>
                dbTrigger.equal(trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            this.diff.createTrigger(trigger);
        }
    }

}
