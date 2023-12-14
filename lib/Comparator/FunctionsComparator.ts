import { AbstractComparator } from "./AbstractComparator";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";

export class FunctionsComparator extends AbstractComparator {

    drop() {
        for (const dbFunc of this.database.functions) {
            
            // ddl-manager cannot drop frozen function
            if ( dbFunc.comment.frozen ) {
                continue;
            }
            if ( dbFunc.cacheSignature ) {
                continue;
            }

            const sameNameFsFunctions = this.fs.getFunctionsByName(dbFunc.name);
            const existsSameFuncFromFile = sameNameFsFunctions.some(fileFunc =>
                fileFunc.equal(dbFunc)
            );

            if ( existsSameFuncFromFile ) {
                continue;
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
        for (const fsFunc of functions) {
            const existsSameFuncFromDb = this.database.getFunctions(fsFunc.name).some(dbFunc =>
                dbFunc.equal(fsFunc)
            );
            if ( existsSameFuncFromDb ) {
                continue;
            }

            this.migration.create({
                functions: [fsFunc]
            });
        }
    }

}