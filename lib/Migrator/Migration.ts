import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { Column } from "../database/schema/Column";
import { Index } from "../database/schema/Index";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";
import { flatMap } from "lodash";

export interface IChanges {
    functions: DatabaseFunction[];
    triggers: DatabaseTrigger[];
    columns: Column[];
    updates: CacheUpdate[];
    indexes: Index[];
}

// tslint:disable: no-console
export class Migration {
    readonly toDrop: IChanges;
    readonly toCreate: IChanges;
    private enabledCacheTriggersOnUpdate: boolean;
    private updateTimeout?: number;

    static empty() {
        return new Migration();
    }

    private constructor() {
        this.enabledCacheTriggersOnUpdate = false;
        this.toDrop = {
            functions: [], triggers: [], columns: [],
            updates: [], indexes: []
        };
        this.toCreate = {
            functions: [], triggers: [], columns: [],
            updates: [], indexes: []
        };
    }

    needDisableCacheTriggersOnUpdate() {
        return !this.enabledCacheTriggersOnUpdate;
    }

    enableCacheTriggersOnUpdate() {
        this.enabledCacheTriggersOnUpdate = true;
    }

    getTimeoutForUpdates() {
        return this.updateTimeout;
    }

    setTimeoutForUpdates(timeout: number) {
        this.updateTimeout = timeout;
    }

    drop(state: Partial<IChanges>) {
        this.toDrop.functions.push(
            ...(state.functions || [])
        );
        this.toDrop.triggers.push(
            ...(state.triggers || [])
        );
        this.toDrop.columns.push(
            ...(state.columns || [])
        );
        this.toDrop.indexes.push(
            ...(state.indexes || [])
        );
        return this;
    }

    create(state: Partial<IChanges>) {
        this.toCreate.functions.push(
            ...(state.functions || [])
        );
        this.toCreate.triggers.push(
            ...(state.triggers || [])
        );
        this.toCreate.columns.push(
            ...(state.columns || [])
        );
        this.toCreate.updates.push(
            ...(state.updates || [])
        );
        this.toCreate.indexes.push(
            ...(state.indexes || [])
        );
        return this;
    }

    log() {
        console.log( new Date().toLocaleTimeString() );

        this.toDrop.triggers.forEach((trigger) => {
            console.log("drop trigger " + trigger.getSignature());
        });
    
        this.toDrop.functions.forEach((func) => {
            console.log("drop function " + func.getSignature());
        });

        this.toDrop.columns.forEach((column) => {
            console.log("drop column " + column.getSignature());
        });
        
        this.toDrop.indexes.forEach((index) => {
            console.log("drop index " + index.getSignature());
        });


        this.toCreate.functions.forEach((func) => {
            console.log("create function " + func.getSignature());
        });
    
        this.toCreate.triggers.forEach((trigger) => {
            console.log("create trigger " + trigger.getSignature());
        });

        this.toCreate.columns.forEach((column) => {
            console.log("create column " + column.getSignature());
        });

        this.toCreate.indexes.forEach((index) => {
            console.log("create index " + index.getSignature());
        });

        this.toCreate.updates.forEach((update) => {
            const columns = flatMap(update.selects, select => select.columns)
                .map(column => column.name);
            console.log(`cache ${update.caches}, update ${update.table.table} set ${columns.join(", ")}`);
        });
    }
    
}