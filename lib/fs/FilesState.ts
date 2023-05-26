import { flatMap } from "lodash";
import { Cache } from "../ast";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { File, IFileParams } from "./File";
import { TableID } from "../database/schema/TableID";

export class FilesState {
    readonly files: File[];
    
    constructor(files: File[] = []) {
        this.files = files;
    }

    allCache() {
        return flatMap(this.files, file => file.content.cache)    
    }

    allTriggers() {
        return flatMap(this.files, file => file.content.triggers);
    }

    getTableTriggers(table: TableID) {
        const tableTriggers = this.allTriggers().filter(trigger => 
            trigger.table.equal(table)
        );
        return tableTriggers;
    }

    addFile(fileOrParams: File | IFileParams) {
        let file!: File;
        if ( fileOrParams instanceof File ) {
            file = fileOrParams;
        }
        else {
            file = new File(fileOrParams);
        }

        this.checkDuplicate( file );
        this.files.push(file);
    }

    removeFile(file: File) {
        const fileIndex = this.files.indexOf(file);
        if ( fileIndex !== -1 ) {
            this.files.splice(fileIndex, 1);
        }
    }

    
    private checkDuplicate(file: File) {
        file.content.functions.forEach(func => 
            this.checkDuplicateFunction( func )
        );
        file.content.triggers.forEach(trigger => {
            this.checkDuplicateTrigger( trigger );
        });
        file.content.cache.forEach(cache => {
            this.checkDuplicateCache( cache );
        });
    }

    private checkDuplicateFunction(func: DatabaseFunction) {
        const identify = func.getSignature();

        const hasDuplicate = this.files.some(someFile => {
            return someFile.content.functions.some((someFunc) => {
                const someIdentify = someFunc.getSignature();
                
                return identify === someIdentify;
            });
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicate function ${ identify }`);
        }
    }

    private checkDuplicateTrigger(trigger: DatabaseTrigger) {
        const identify = trigger.getSignature();

        const hasDuplicate = this.files.some(someFile => {
            const someTriggers = someFile.content.triggers;

            if ( someTriggers ) {
                return someTriggers.some((someTrigger) => {
                    const someIdentify = someTrigger.getSignature();

                    return identify === someIdentify;
                });
            }
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicate trigger ${ identify }`);
        }
    }

    private checkDuplicateCache(cache: Cache) {
        const identify = cache.getSignature();
        const cacheColumns = cache.select.columns.map(col => col.name);;

        for (const someFile of this.files) {
            for (const someCache of someFile.content.cache) {
                // duplicated cache name
                if ( someCache.getSignature() === identify ) {
                    throw new Error(`duplicate ${ identify }`);
                }
                // duplicate cache columns

                if ( someCache.for.table.equal(cache.for.table) ) {
                    const someCacheColumns = someCache.select.columns.map(col => col.name);
                    const duplicatedColumns = someCacheColumns.filter(columnName =>
                        cacheColumns.includes(columnName)
                    );

                    if ( duplicatedColumns.length > 0 ) {
                        throw new Error(`duplicated columns: ${ duplicatedColumns } by cache: ${cache.name}, ${someCache.name}`);
                    }
                }
            }
        }
    }

}