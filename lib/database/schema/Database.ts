import { flatMap } from "lodash";
import { DatabaseFunction, DatabaseTrigger } from "../../ast";
import { Table, ITableID } from "./Table";

export class Database {
    readonly tables: Table[];
    readonly functions: DatabaseFunction[];
    
    constructor(tables: Table[] = []) {
        this.tables = tables;
        this.functions = [];
    }

    getTable(tableId: ITableID) {
        return this.tables.find(table => 
            table.name === tableId.name &&
            table.schema === tableId.schema
        );
    }

    getTriggersByProcedure(procedure: {schema: string, name: string, args: string[]}) {
        return flatMap(this.tables, (table) => {
            return table.triggers.filter(trigger =>
                trigger.procedure.schema === procedure.schema &&
                trigger.procedure.name === procedure.name
            );
        });
    }

    setTable(table: Table) {
        const hasTable = this.getTable(table);
        if ( !hasTable ) {
            this.tables.push(table);
        }
    }

    addFunctions(functions: DatabaseFunction[]) {
        this.functions.push(
            ...functions
        );
    }

    addTrigger(trigger: DatabaseTrigger) {
        const table = this.getTable(trigger.table);
        if ( !table ) {
            throw new Error(`unknown table "${ trigger.table.schema }.${ trigger.table.name }" for trigger "${ trigger.name }"`)
        }

        table.addTrigger(trigger);
    }
}