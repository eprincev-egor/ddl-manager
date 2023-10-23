import { flatMap } from "lodash";
import { Cache } from "../ast";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { File, IFileParams } from "./File";
import { TableID } from "../database/schema/TableID";

export class FilesState {
    readonly files: File[];
    private cacheMap: Record<string, Cache[]> = {};
    private functionsMap: Record<string, DatabaseFunction[]> = {};
    private triggersMap: Record<string, DatabaseTrigger[]> = {};
    
    constructor(files: File[] = []) {
        this.files = files;
    }

    allNotHelpersFiles() {
        return this.files.filter(file => 
            file.folder !== "HELPERS" &&
            !file.name.startsWith("CM_")
        );
    }

    allCache() {
        return flatMap(this.files, file => file.content.cache)    
    }

    allTriggers() {
        return flatMap(this.files, file => file.content.triggers);
    }

    allFunctions() {
        return flatMap(this.files, file => file.content.functions);
    }

    allNotHelperFunctions() {
        return flatMap(this.allNotHelpersFiles(), file => file.content.functions);
    }

    getCachesForTable(forTable: TableID) {
        return (this.cacheMap[ forTable.toString() ] ?? []).slice();
    }

    getFunctionsByName(name: string) {
        return (this.functionsMap[ name ] ?? []).slice();
    }

    getTableTriggers(table: TableID) {
        return (this.triggersMap[ table.toString() ] ?? []).slice();
    }

    getTriggerFunction(trigger: DatabaseTrigger) {
        return (this.functionsMap[trigger.procedure.name] || []).find(func =>
            func.schema === trigger.procedure.schema
        );
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

        for (const func of file.content.functions) {
            this.functionsMap[ func.name ] ??= [];
            this.functionsMap[ func.name ].push(func);
        }

        for (const trigger of file.content.triggers) {
            const table = trigger.table.toString();
            this.triggersMap[ table ] ??= [];
            this.triggersMap[ table ].push(trigger);
        }

        for (const cache of file.content.cache) {
            const table = cache.for.table.toString();
            this.cacheMap[ table ] ??= [];
            this.cacheMap[ table ].push(cache);
        }
    }

    removeFile(file: File) {
        const fileIndex = this.files.indexOf(file);
        if ( fileIndex !== -1 ) {
            this.files.splice(fileIndex, 1);
        }

        for (const deletedFunc of file.content.functions) {
            this.functionsMap[ deletedFunc.name ] ??= [];
            this.functionsMap[ deletedFunc.name ] = this.functionsMap[ deletedFunc.name ]
                .filter(func => func.getSignature() != deletedFunc.getSignature());
        }

        for (const deletedTrigger of file.content.triggers) {
            const table = deletedTrigger.table.toString();
            this.triggersMap[ table ] ??= [];
            this.triggersMap[ table ] = this.triggersMap[ table ]
                .filter(trigger => trigger.getSignature() != deletedTrigger.getSignature());
        }

        for (const deletedCache of file.content.cache) {
            const table = deletedCache.for.table.toString();
            this.cacheMap[ table ] ??= [];
            this.cacheMap[ table ] = this.cacheMap[ table ]
                .filter(cache => cache.getSignature() !== deletedCache.getSignature())
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

        const hasDuplicate = this.getFunctionsByName(func.name).some(someFunc => {
            const someIdentify = someFunc.getSignature();
            return identify === someIdentify;
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicated function ${ identify }`);
        }
    }

    private checkDuplicateTrigger(trigger: DatabaseTrigger) {
        const identify = trigger.getSignature();

        const hasDuplicate = this.getTableTriggers(trigger.table).some(someTrigger => {
            const someIdentify = someTrigger.getSignature();
            return identify === someIdentify;
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicated trigger ${ identify }`);
        }
    }

    private checkDuplicateCache(cache: Cache) {
        const identify = cache.getSignature();
        const cacheColumns = cache.select.columns.map(col => col.name);;

        for (const someCache of this.getCachesForTable(cache.for.table)) {
            // duplicated cache name
            if ( someCache.getSignature() === identify ) {
                throw new Error(`duplicated ${ identify }`);
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