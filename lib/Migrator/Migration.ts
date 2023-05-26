import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { Column } from "../database/schema/Column";
import { Index } from "../database/schema/Index";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";
import { flatMap } from "lodash";
import fs from "fs";

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
    private updatePackageSize = 20000;
    private logFilePath?: string;

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

    getUpdatePackageSize() {
        return this.updatePackageSize;
    }

    setUpdatePackageSize(packageSize: number) {
        this.updatePackageSize = packageSize;
    }

    unDropTrigger(trigger: DatabaseTrigger) {
        this.toDrop.triggers = this.toDrop.triggers.filter(droppedTrigger =>
            droppedTrigger.getSignature() !== trigger.getSignature()
        );
    }

    unCreateTrigger(trigger: DatabaseTrigger) {
        this.toCreate.triggers = this.toCreate.triggers.filter(droppedTrigger =>
            droppedTrigger.getSignature() !== trigger.getSignature()
        );
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
        this.toCreate.columns.push(
            ...(state.columns || [])
        );
        this.toCreate.updates.push(
            ...(state.updates || [])
        );
        this.toCreate.indexes.push(
            ...(state.indexes || [])
        );

        for (const newFunc of (state.functions || [])) {
            this.toCreate.functions = this.toCreate.functions.filter(func =>
                func.getSignature() !== newFunc.getSignature()
            );
            this.toCreate.functions.push(newFunc);
        }

        for (const newTrigger of (state.triggers || [])) {
            this.toCreate.triggers = this.toCreate.triggers.filter(trigger =>
                trigger.getSignature() !== newTrigger.getSignature()
            );
            this.toCreate.triggers.push(newTrigger);
        }

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

    logToFile(filePath: string) {
        this.logFilePath = filePath;
    }
    
    addLog(log: string) {
        console.log(log);

        if ( this.logFilePath ){ 
            fs.appendFileSync(this.logFilePath, log + "\n");
        }
    }
}