import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { Column } from "../database/schema/Column";
import { TableReference } from "../database/schema/TableReference";
import { Select } from "../ast";

interface IUpdate {
    select: Select;
    forTable: TableReference;
}

interface IChanges {
    functions: DatabaseFunction[];
    triggers: DatabaseTrigger[];
    columns: Column[];
    updates: IUpdate[];
}

// tslint:disable: no-console
export class Migration {
    readonly toDrop: IChanges;
    readonly toCreate: IChanges;

    static empty() {
        return new Migration();
    }

    private constructor() {
        this.toDrop = {functions: [], triggers: [], columns: [], updates: []};
        this.toCreate = {functions: [], triggers: [], columns: [], updates: []};
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
        this.toDrop.updates.push(
            ...(state.updates || [])
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
        return this;
    }

    log() {
        this.toDrop.triggers.forEach((trigger) => {
            console.log("drop trigger " + trigger.getSignature());
        });
    
        this.toDrop.functions.forEach((func) => {
            console.log("drop function " + func.getSignature());
        });
        

        this.toCreate.functions.forEach((func) => {
            console.log("create function " + func.getSignature());
        });
    
        this.toCreate.triggers.forEach((trigger) => {
            console.log("create trigger " + trigger.getSignature());
        });
    }
    
}