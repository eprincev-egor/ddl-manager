import { AbstractComparator } from "./AbstractComparator";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { flatMap } from "lodash";

export class FunctionsComparator extends AbstractComparator {

    drop() {
        for (const dbFunc of this.database.functions) {
            
            // ddl-manager cannot drop frozen function
            if ( dbFunc.frozen ) {
                continue;
            }
            if ( dbFunc.cacheSignature ) {
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

    create() {
        for (const file of this.fs.files) {
            this.createNewFunctions( file.content.functions );
        }
    }

    createLogFuncs() {
        for (const file of this.fs.files) {
            for (const func of file.content.functions) {
                this.migration.create({
                    functions: [func]
                });
            }
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

}